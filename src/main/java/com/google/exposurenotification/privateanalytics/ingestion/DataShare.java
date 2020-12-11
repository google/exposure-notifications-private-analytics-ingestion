/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.exposurenotification.privateanalytics.ingestion;

import com.google.auto.value.AutoValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Value.ValueTypeCase;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Pipeline view of Firestore documents corresponding to Prio data share pairs. */
@AutoValue
public abstract class DataShare implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DataShare.class);

  private static final long serialVersionUID = 1L;
  public static final int LATEST_SCHEMA_VERSION = 2;

  private static final Counter missingRequiredCounter =
      Metrics.counter(DataShare.class, "datashare-missingRequired");
  private static final Counter castExceptionCounter =
      Metrics.counter(DataShare.class, "datashare-castException");
  private static final Counter illegalArgCounter =
      Metrics.counter(DataShare.class, "datashare-illegalArg");
  private static final Counter failedRPitGenerationCounter =
      Metrics.counter(DataShare.class, "datashare-failedRPitGeneration");
  private static final Counter invalidDocumentCounter =
      Metrics.counter(DataShare.class, "datashare-invalidDocument");

  // Firestore document field names. See
  // https://github.com/google/exposure-notifications-android/blob/4b7b461282b2ede6fb2a93488c6d628440052c8d/app/src/main/java/com/google/android/apps/exposurenotification/privateanalytics/PrivateAnalyticsFirestoreRepository.java#L44
  public static final String DOCUMENT_FIELDS = "documentFields";
  public static final String PAYLOAD = "payload";
  public static final String SIGNATURE = "signature";
  public static final String CERT_CHAIN = "certificateChain";

  // Payload fields
  public static final String SCHEMA_VERSION = "schemaVersion";
  public static final String ENCRYPTED_DATA_SHARES = "encryptedDataShares";
  public static final String CREATED = "created";
  public static final String UUID = "uuid";
  public static final String PRIO_PARAMS = "prioParams";

  // Signature and certificates fields
  public abstract @Nullable String getSignature();

  public abstract @Nullable List<String> getCertificateChain();

  // Prio Parameters field names
  public static final String PRIME_FIELD = "prime";
  public static final String BINS = "bins";
  public static final String EPSILON = "epsilon";
  public static final String NUMBER_OF_SERVERS_FIELD = "numberServers";
  public static final String HAMMING_WEIGHT = "hammingWeight";

  // Prio hardcoded parameters
  public static final long PRIME = 4293918721L;
  public static final int NUMBER_OF_SERVERS = 2;

  // Encrypted Data Share fields
  public static final String ENCRYPTION_KEY_ID = "encryptionKeyId";
  public static final String DATA_SHARE_PAYLOAD = "payload";

  /** Firestore document path */
  public abstract @Nullable String getPath();

  public abstract @Nullable Long getCreatedMs();

  public abstract @Nullable String getUuid();

  public abstract @Nullable String getException();

  public abstract @Nullable Long getRPit();

  public abstract @Nullable Integer getSchemaVersion();

  public abstract @Nullable List<EncryptedShare> getEncryptedDataShares();

  public abstract @Nullable DataShareMetadata getDataShareMetadata();

  /** @return Pipeline projection of Firestore document */
  public static DataShare from(Document doc) {
    DataShare.Builder builder = builder();
    builder.setPath(doc.getName());

    // Process the payload.
    if (doc.getFieldsMap().get(PAYLOAD) == null) {
      missingRequiredCounter.inc();
      throw new InvalidDataShareException("Missing required field: " + PAYLOAD);
    }
    Map<String, Value> payload = doc.getFieldsMap().get(PAYLOAD).getMapValue().getFieldsMap();

    checkValuePresent(CREATED, payload, PAYLOAD, ValueTypeCase.TIMESTAMP_VALUE);
    checkValuePresent(UUID, payload, PAYLOAD, ValueTypeCase.STRING_VALUE);
    builder.setCreatedMs(
        payload.get(CREATED).getTimestampValue().getSeconds() * 1000
            + payload.get(CREATED).getTimestampValue().getNanos() / 1000000);
    builder.setUuid(payload.get(UUID).getStringValue());

    // Check against the latest schema version.
    // In the future, if we want to support different schema versions, the logic may need to change
    // according to the value in the payload.
    if (payload.get(SCHEMA_VERSION) == null) {
      missingRequiredCounter.inc();
      throw new InvalidDataShareException("Missing required field: " + SCHEMA_VERSION);
    }
    Integer schemaVersion = (int) payload.get(SCHEMA_VERSION).getIntegerValue();
    if (schemaVersion > LATEST_SCHEMA_VERSION || schemaVersion <= 0) {
      illegalArgCounter.inc();
      throw new InvalidDataShareException("Invalid schema version: " + schemaVersion);
    }
    builder.setSchemaVersion(schemaVersion);

    // Get the Prio parameters.
    DataShareMetadata.Builder metadataBuilder = DataShareMetadata.builder();
    checkValuePresent(PRIO_PARAMS, payload, PAYLOAD, ValueTypeCase.MAP_VALUE);
    Map<String, Value> prioParams = payload.get(PRIO_PARAMS).getMapValue().getFieldsMap();

    checkValuePresent(PRIME_FIELD, prioParams, PRIO_PARAMS, ValueTypeCase.INTEGER_VALUE);
    Long prime = prioParams.get(PRIME_FIELD).getIntegerValue();
    if (prime != PRIME) {
      illegalArgCounter.inc();
      throw new InvalidDataShareException("Invalid prime: " + prime);
    }
    metadataBuilder.setPrime(prime);

    checkValuePresent(EPSILON, prioParams, PRIO_PARAMS, ValueTypeCase.DOUBLE_VALUE);
    metadataBuilder.setEpsilon(prioParams.get(EPSILON).getDoubleValue());

    checkValuePresent(BINS, prioParams, PRIO_PARAMS, ValueTypeCase.INTEGER_VALUE);
    int bins = (int) prioParams.get(BINS).getIntegerValue();
    if (bins < 0) {
      illegalArgCounter.inc();
      throw new InvalidDataShareException("Invalid number of bins: " + bins);
    }
    metadataBuilder.setBins(bins);

    checkValuePresent(
        NUMBER_OF_SERVERS_FIELD, prioParams, PRIO_PARAMS, ValueTypeCase.INTEGER_VALUE);
    int numberOfServers = (int) prioParams.get(NUMBER_OF_SERVERS_FIELD).getIntegerValue();
    if (numberOfServers != NUMBER_OF_SERVERS) {
      illegalArgCounter.inc();
      throw new InvalidDataShareException("Invalid number of servers: " + numberOfServers);
    }
    metadataBuilder.setNumberOfServers(numberOfServers);

    if (prioParams.get(HAMMING_WEIGHT) != null) {
      // This will type-check the hamming weight field.
      checkValuePresent(HAMMING_WEIGHT, prioParams, PRIO_PARAMS, ValueTypeCase.INTEGER_VALUE);
      metadataBuilder.setHammingWeight((int) prioParams.get(HAMMING_WEIGHT).getIntegerValue());
    }
    try {
      String fullPath = doc.getName();
      // The metricName is the base name of the document path
      metadataBuilder.setMetricName(fullPath.substring(fullPath.lastIndexOf('/') + 1));
    } catch (RuntimeException e) {
      missingRequiredCounter.inc();
      throw new InvalidDataShareException("Missing required field: Name", e);
    }

    builder.setDataShareMetadata(metadataBuilder.build());

    // Generate a r_PIT randomly for every data share. r_PIT cannot be equal to any of the n-th
    // root of unity where n = next_power_two(#bins + 1).
    // Cf. page 18 of https://eprint.iacr.org/2019/188.pdf.
    Long rPit;
    try {
      BigInteger N = BigInteger.valueOf(nextPowerTwo(bins + 1));
      BigInteger P = BigInteger.valueOf(prime);
      rPit = generateRandom(prime);
      while (BigInteger.valueOf(rPit).modPow(N, P) == BigInteger.ONE) {
        rPit = generateRandom(prime);
      }
    } catch (RuntimeException e) {
      failedRPitGenerationCounter.inc();
      throw new InvalidDataShareException("Could not generate rPit", e);
    }
    builder.setRPit(rPit);

    // Get the encrypted shares.
    checkValuePresent(ENCRYPTED_DATA_SHARES, payload, PAYLOAD, ValueTypeCase.ARRAY_VALUE);
    List<Value> encryptedDataShares =
        payload.get(ENCRYPTED_DATA_SHARES).getArrayValue().getValuesList();
    if (encryptedDataShares.size() != numberOfServers) {
      illegalArgCounter.inc();
      throw new InvalidDataShareException(
          "Mismatch between number of servers ("
              + numberOfServers
              + ") and number of data shares ("
              + encryptedDataShares.size()
              + ")");
    }
    List<EncryptedShare> shares = new ArrayList<>(NUMBER_OF_SERVERS);
    // Ensure data shares are of correct type and convert to DataShare-compatible type.
    for (int i = 0; i < encryptedDataShares.size(); i++) {
      Map<String, Value> encryptedDataShare =
          encryptedDataShares.get(i).getMapValue().getFieldsMap();
      checkValuePresent(
          ENCRYPTION_KEY_ID,
          encryptedDataShare,
          ENCRYPTED_DATA_SHARES + "[" + i + "]",
          ValueTypeCase.STRING_VALUE);
      checkValuePresent(
          DATA_SHARE_PAYLOAD,
          encryptedDataShare,
          ENCRYPTED_DATA_SHARES + "[" + i + "]",
          ValueTypeCase.STRING_VALUE);
      String keyId = encryptedDataShare.get(ENCRYPTION_KEY_ID).getStringValue();
      String base64payload = encryptedDataShare.get(DATA_SHARE_PAYLOAD).getStringValue();
      byte[] decodedPayload;
      try {
        decodedPayload = Base64.getDecoder().decode(base64payload);
      } catch (IllegalArgumentException e) {
        illegalArgCounter.inc();
        throw new InvalidDataShareException("Unable to base64 decode payload", e);
      }
      shares.add(
          EncryptedShare.builder()
              .setEncryptionKeyId(keyId)
              .setEncryptedPayload(decodedPayload)
              .build());
    }
    builder.setEncryptedDataShares(shares);

    // Get the signature and cert chain
    Map<String, Value> fields = doc.getFieldsMap();
    checkValuePresent(SIGNATURE, fields, DOCUMENT_FIELDS, ValueTypeCase.STRING_VALUE);
    builder.setSignature(fields.get(SIGNATURE).getStringValue());
    if (fields.get(CERT_CHAIN) == null) {
      missingRequiredCounter.inc();
      throw new InvalidDataShareException("Missing required field: " + CERT_CHAIN);
    }
    List<Value> certChainValue = fields.get(CERT_CHAIN).getArrayValue().getValuesList();
    List<String> certChainString = new ArrayList<>();
    for (Value cert : certChainValue) {
      if (cert.getStringValue() == null) {
        illegalArgCounter.inc();
        throw new InvalidDataShareException("invalid or empty certificate");
      }
      certChainString.add(cert.getStringValue());
    }
    builder.setCertificateChain(certChainString);

    return builder.build();
  }

  static Builder builder() {
    return new AutoValue_DataShare.Builder();
  }

  public static class InvalidDataShareException extends IllegalArgumentException {

    public InvalidDataShareException(String s) {
      super(s);
    }

    public InvalidDataShareException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract DataShare build();

    abstract Builder setPath(@Nullable String value);

    abstract Builder setCreatedMs(@Nullable Long value);

    abstract Builder setUuid(@Nullable String value);

    abstract Builder setException(@Nullable String value);

    abstract Builder setRPit(@Nullable Long value);

    abstract Builder setSchemaVersion(@Nullable Integer value);

    abstract Builder setEncryptedDataShares(@Nullable List<EncryptedShare> value);

    abstract Builder setDataShareMetadata(@Nullable DataShareMetadata value);

    abstract Builder setSignature(@Nullable String value);

    abstract Builder setCertificateChain(@Nullable List<String> certChain);
  }

  // A transform that constructs and outputs a DataShare for a Document.
  static class ConstructDataSharesFn extends DoFn<Document, DataShare> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      try {
        DataShare dataShare = DataShare.from(context.element());
        Metrics.counter(DataShare.class, "dataShares").inc();
        Metrics.counter(
                DataShare.class, "dataShares-" + dataShare.getDataShareMetadata().getMetricName())
            .inc();
        context.output(dataShare);
      } catch (InvalidDataShareException e) {
        LOG.warn("Invalid data share", e);
        invalidDocumentCounter.inc();
      }
    }
  }

  // Next power of two of an int.
  public static long nextPowerTwo(int n) throws IllegalArgumentException {
    if (n < 0) {
      throw new InvalidDataShareException("n cannot be < 0.");
    }
    int count = 0;
    // Test if n is a non-zero power of 2.
    if (n > 0 && (n & (n - 1)) == 0) {
      return (long) n;
    }
    while (n != 0) {
      n >>= 1;
      count += 1;
    }
    return 1L << count;
  }

  // Checks for the presence of the given field in the sourceMap and provides detailed exceptions
  // if the field is absent or of the wrong type.
  private static void checkValuePresent(
      String field, Map<String, Value> sourceMap, String sourceName, ValueTypeCase type) {
    if (!sourceMap.containsKey(field) || sourceMap.get(field) == null) {
      missingRequiredCounter.inc();
      throw new InvalidDataShareException(
          "Missing required field: '" + field + "' from '" + sourceName + "'");
    }

    if (!sourceMap.get(field).getValueTypeCase().equals(type)) {
      castExceptionCounter.inc();
      throw new InvalidDataShareException(
          "Error casting '" + field + "' from '" + sourceName + "' to " + type.name());
    }
  }

  // Generate a random element in [0, p-1] using SecureRandom.
  private static Long generateRandom(Long p) throws IllegalArgumentException {
    if (p <= 0) {
      throw new InvalidDataShareException("The upper bound should be > 0.");
    }
    // Use rejection sampling to generate a random v in [0, p-1].
    // We generate a v with the same number of bits as p, and restart until v is
    // smaller than p.
    SecureRandom secureRandom = new SecureRandom();
    Long v = Long.MAX_VALUE;
    while (v >= p || v < 0) { // this terminates in less than 4 rounds in expectation.
      v = secureRandom.nextLong();
      v >>= Long.numberOfLeadingZeros(p);
    }
    return v;
  }

  /** Represents the grouping key by which data shares should be aggregated together. */
  @AutoValue
  public abstract static class DataShareMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract @Nullable Double getEpsilon();

    public abstract @Nullable Long getPrime();

    public abstract @Nullable Integer getBins();

    public abstract @Nullable Integer getNumberOfServers();

    public abstract @Nullable Integer getHammingWeight();

    public abstract @Nullable String getMetricName();

    public abstract @Nullable Integer getBatchNumber();

    public abstract DataShareMetadata.Builder toBuilder();

    static DataShareMetadata.Builder builder() {
      return new AutoValue_DataShare_DataShareMetadata.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      abstract DataShareMetadata build();

      abstract Builder setEpsilon(@Nullable Double value);

      abstract Builder setPrime(@Nullable Long value);

      abstract Builder setBins(@Nullable Integer value);

      abstract Builder setNumberOfServers(@Nullable Integer value);

      abstract Builder setHammingWeight(@Nullable Integer value);

      abstract Builder setMetricName(@Nullable String value);

      abstract Builder setBatchNumber(@Nullable Integer value);
    }
  }

  /** Represents the core encrypted Prio data payload */
  @AutoValue
  public abstract static class EncryptedShare implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract @Nullable byte[] getEncryptedPayload();

    public abstract @Nullable String getEncryptionKeyId();

    static EncryptedShare.Builder builder() {
      return new AutoValue_DataShare_EncryptedShare.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      abstract EncryptedShare build();

      abstract Builder setEncryptedPayload(byte[] value);

      abstract Builder setEncryptionKeyId(String value);
    }
  }
}
