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
import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Pipeline view of Firestore documents corresponding to Prio data share pairs.
 */
@DefaultCoder(AvroCoder.class)
@AutoValue
public abstract class DataShare implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final int NUMBER_OF_SERVERS = 2;
  private static final String ROOT_COLLECTION_NAME = "uuid";
  private static final Counter missingRequiredCounter = Metrics
      .counter(DataShare.class, "datashare-missingRequired");
  private static final Counter castExceptionCounter = Metrics
      .counter(DataShare.class, "datashare-castException");
  private static final Counter illegalArgCounter = Metrics
      .counter(DataShare.class, "datashare-illegalArg");

  // Firestore document field names. See
  // https://github.com/google/exposure-notifications-android/tree/master/app/src/main/java/com/google/android/apps/exposurenotification/privateanalytics/PrivateAnalyticsFirestoreRepository.java#50
  public static final String DOCUMENT_FIELDS = "documentFields";
  public static final String PAYLOAD = "payload";
  public static final String SIGNATURE = "signature";
  public static final String CERT_CHAIN = "certificateChain";

  // Payload fields
  public static final String ENCRYPTED_DATA_SHARES = "encryptedDataShares";
  public static final String CREATED = "created";
  public static final String UUID = "uuid";
  public static final String PRIO_PARAMS = "prioParams";
  // TODO: delete this field prior to launch/open source - this is just a convenient way to analyze exceptions from different test devices
  public static final String EXCEPTION = "exception";

  // Signature and certificates fields
  public abstract @Nullable String getSignature();

  public abstract @Nullable List<X509Certificate> getCertificateChain();

  // Prio Parameters field names
  public static final String PRIME = "prime";
  public static final String BINS = "bins";
  public static final String EPSILON = "epsilon";
  public static final String NUMBER_OF_SERVERS_FIELD = "numberServers";
  public static final String HAMMING_WEIGHT = "hammingWeight";

  // Encrypted Data Share fields
  public static final String ENCRYPTION_KEY_ID = "encryptionKeyId";
  public static final String DATA_SHARE_PAYLOAD = "payload";

  /**
   * Firestore document path
   */
  public abstract @Nullable String getPath();

  public abstract @Nullable Long getCreated();

  public abstract @Nullable String getUuid();

  public abstract @Nullable String getException();

  public abstract @Nullable Long getRPit();

  public abstract @Nullable List<EncryptedShare> getEncryptedDataShares();

  public abstract @Nullable DataShareMetadata getDataShareMetadata();


  /**
   * @return Pipeline projection of Firestore document
   */
  public static DataShare from(Document doc) {
    DataShare.Builder builder = builder();
    try {
      // doc.getName() returns the fully qualified name of the document:
      // e.g.: projects/{project_id}/databases/{database_id}/documents/uuid/.../metricName
      // We need the path relative to the beginning of the root collection, "uuid/"
      builder.setPath(doc.getName().substring(doc.getName().indexOf(ROOT_COLLECTION_NAME)));
    } catch (RuntimeException e) {
      missingRequiredCounter.inc();
      throw new IllegalArgumentException("Missing required field: Path", e);
    }

    // Step 1: Process the payload.
    if (doc.getFieldsMap().get(PAYLOAD) == null) {
      missingRequiredCounter.inc();
      throw new IllegalArgumentException("Missing required field: " + PAYLOAD);
    }
    Map<String, Value> payload = doc.getFieldsMap().get(PAYLOAD).getMapValue().getFieldsMap();

    checkValuePresent(CREATED, payload, PAYLOAD, ValueTypeCase.TIMESTAMP_VALUE);
    checkValuePresent(UUID, payload, PAYLOAD, ValueTypeCase.STRING_VALUE);
    builder.setCreated(payload.get(CREATED).getTimestampValue().getSeconds());
    builder.setUuid(payload.get(UUID).getStringValue());

    // Get the Prio parameters.
    checkValuePresent(PRIO_PARAMS, payload, PAYLOAD, ValueTypeCase.MAP_VALUE);
    Map<String, Value> prioParams = payload.get(PRIO_PARAMS).getMapValue().getFieldsMap();
    checkValuePresent(PRIME, prioParams, PRIO_PARAMS, ValueTypeCase.INTEGER_VALUE);
    Long prime = prioParams.get(PRIME).getIntegerValue();

    DataShareMetadata.Builder metadataBuilder = DataShareMetadata.builder();
    metadataBuilder.setPrime(prime);
    checkValuePresent(EPSILON, prioParams, PRIO_PARAMS, ValueTypeCase.DOUBLE_VALUE);
    metadataBuilder.setEpsilon(prioParams.get(EPSILON).getDoubleValue());
    checkValuePresent(BINS, prioParams, PRIO_PARAMS, ValueTypeCase.INTEGER_VALUE);
    metadataBuilder.setBins((int) prioParams.get(BINS).getIntegerValue());
    checkValuePresent(NUMBER_OF_SERVERS_FIELD, prioParams, PRIO_PARAMS, ValueTypeCase.INTEGER_VALUE);
    int numberOfServers = (int) prioParams.get(NUMBER_OF_SERVERS_FIELD).getIntegerValue();
    if (numberOfServers != NUMBER_OF_SERVERS) {
      illegalArgCounter.inc();
      throw new UnsupportedOperationException("Unsupported number of servers: " + numberOfServers);
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
      throw new IllegalArgumentException("Missing required field: Name", e);
    }

    builder.setDataShareMetadata(metadataBuilder.build());

    // Generate a r_PIT randomly for every data share.
    Long rPit = generateRandom(prime);
    builder.setRPit(rPit);

    // Get the encrypted shares.
    checkValuePresent(ENCRYPTED_DATA_SHARES, payload, PAYLOAD, ValueTypeCase.ARRAY_VALUE);
    List<Value> encryptedDataShares = payload.get(ENCRYPTED_DATA_SHARES).getArrayValue().getValuesList();
    if (encryptedDataShares.size() != numberOfServers) {
      illegalArgCounter.inc();
      throw new IllegalArgumentException("Mismatch between number of servers (" + numberOfServers
          + ") and number of data shares (" + encryptedDataShares.size() + ")");
    }
    List<EncryptedShare> shares = new ArrayList<>(NUMBER_OF_SERVERS);
    // Ensure data shares are of correct type and convert to DataShare-compatible type.
    for (int i = 0; i < encryptedDataShares.size(); i++) {
      Map<String, Value> encryptedDataShare = encryptedDataShares.get(i).getMapValue().getFieldsMap();
      checkValuePresent(ENCRYPTION_KEY_ID, encryptedDataShare, ENCRYPTED_DATA_SHARES + "[" + i + "]", ValueTypeCase.STRING_VALUE);
      checkValuePresent(DATA_SHARE_PAYLOAD, encryptedDataShare, ENCRYPTED_DATA_SHARES + "[" + i + "]", ValueTypeCase.STRING_VALUE);
      String keyId = encryptedDataShare.get(ENCRYPTION_KEY_ID).getStringValue();
      String base64payload = encryptedDataShare.get(DATA_SHARE_PAYLOAD).getStringValue();
      byte[] decodedPayload;
      try {
        decodedPayload = Base64.getDecoder().decode(base64payload);
      } catch (IllegalArgumentException e) {
        illegalArgCounter.inc();
        throw new IllegalArgumentException("Unable to base64 decode payload", e);
      }
      shares.add(EncryptedShare.builder()
          .setEncryptionKeyId(keyId)
          .setEncryptedPayload(decodedPayload)
          .build());
    }
    builder.setEncryptedDataShares(shares);

    // Step 2: Get the exception message (if it's present)
    Map<String, Value> fields = doc.getFieldsMap();
    if (fields.containsKey(EXCEPTION) && fields.get(EXCEPTION) != null) {
      if (!fields.get(EXCEPTION).getValueTypeCase().equals(ValueTypeCase.STRING_VALUE)) {
        castExceptionCounter.inc();
        throw new IllegalArgumentException(
            "Error casting 'exception' from 'payload' to string");
      }
      builder.setException(fields.get(EXCEPTION).getStringValue());
      // If the exception is present, no need to check for signature or certificates
      return builder.build();
    }

    // Step 3: Get the signature.
    checkValuePresent(SIGNATURE, fields, DOCUMENT_FIELDS, ValueTypeCase.STRING_VALUE);
    builder.setSignature(fields.get(SIGNATURE).getStringValue());

    // Step 4: Get the chain of X509 certificates.
    if (fields.get(CERT_CHAIN) == null) {
      missingRequiredCounter.inc();
      throw new IllegalArgumentException("Missing required field: " + CERT_CHAIN);
    }
    List<Value> certChainString = fields.get(CERT_CHAIN).getArrayValue().getValuesList();

    List<X509Certificate> certChainX509 = new ArrayList<>();
    try {
      CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
      for (Value certString : certChainString) {
        byte[] cert = Base64.getDecoder().decode(certString.getStringValue());
        // Parse as X509 certificate.
        // TODO figure out why this throws 'CertificateException: Could not parse certificate:
        //  java.io.IOException: Empty input' for all documents
        X509Certificate certX509 = (X509Certificate) certFactory.generateCertificate(new ByteArrayInputStream(cert));
        certChainX509.add(certX509);
       }
    } catch (Exception e) {
      illegalArgCounter.inc();
      throw new IllegalArgumentException("Could not parse the chain of certificates: " + CERT_CHAIN,
          e);
    }

    builder.setCertificateChain(certChainX509);

    return builder.build();
  }

  static Builder builder() {
    return new AutoValue_DataShare.Builder();
  }

  @AutoValue.Builder
  @DefaultCoder(AvroCoder.class)
  abstract static class Builder {

    abstract DataShare build();

    abstract Builder setPath(@Nullable String value);

    abstract Builder setCreated(@Nullable Long value);

    abstract Builder setUuid(@Nullable String value);

    abstract Builder setException(@Nullable String value);

    abstract Builder setRPit(@Nullable Long value);

    abstract Builder setEncryptedDataShares(@Nullable List<EncryptedShare> value);

    abstract Builder setDataShareMetadata(@Nullable DataShareMetadata value);

    abstract Builder setSignature(@Nullable String value);

    abstract Builder setCertificateChain(@Nullable List<X509Certificate> certChain);
  }

  // Checks for the presence of the given field in the sourceMap and provides detailed exceptions
  // if the field is absent or of the wrong type.
  private static void checkValuePresent(String field,  Map<String, Value> sourceMap, String sourceName, ValueTypeCase type) {
    if (!sourceMap.containsKey(field) || sourceMap.get(field) == null) {
      missingRequiredCounter.inc();
      throw new IllegalArgumentException("Missing required field: '" + field + "' from '" + sourceName + "'");
    }

    if (!sourceMap.get(field).getValueTypeCase().equals(type)) {
      castExceptionCounter.inc();
      throw new IllegalArgumentException(
          "Error casting '" + field + "' from '" + sourceName + "' to " + type.name());
    }
  }

  // Generate a random element in [0, p-1] using SecureRandom.
  private static Long generateRandom(Long p) throws IllegalArgumentException {
    if (p <= 0) {
      throw new IllegalArgumentException("The upper bound should be > 0.");
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

  /**
   * Represents the grouping key by which data shares should be aggregated together.
   */
  @AutoValue
  @DefaultCoder(AvroCoder.class)
  public static abstract class DataShareMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract @Nullable Double getEpsilon();

    public abstract @Nullable Long getPrime();

    public abstract @Nullable Integer getBins();

    public abstract @Nullable Integer getNumberOfServers();

    public abstract @Nullable Integer getHammingWeight();

    public abstract @Nullable String getMetricName();

    static DataShareMetadata.Builder builder() {
      return new AutoValue_DataShare_DataShareMetadata.Builder();
    }

    @AutoValue.Builder
    @DefaultCoder(AvroCoder.class)
    public static abstract class Builder {

      abstract DataShareMetadata build();

      abstract Builder setEpsilon(@Nullable Double value);

      abstract Builder setPrime(@Nullable Long value);

      abstract Builder setBins(@Nullable Integer value);

      abstract Builder setNumberOfServers(@Nullable Integer value);

      abstract Builder setHammingWeight(@Nullable Integer value);

      abstract Builder setMetricName(@Nullable String value);
    }
  }

  /**
   * Represents the core encrypted Prio data payload
   */
  @AutoValue
  public static abstract class EncryptedShare implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract @Nullable byte[] getEncryptedPayload();

    public abstract @Nullable String getEncryptionKeyId();

    static EncryptedShare.Builder builder() {
      return new AutoValue_DataShare_EncryptedShare.Builder();
    }

    @AutoValue.Builder
    public static abstract class Builder {

      abstract EncryptedShare build();

      abstract Builder setEncryptedPayload(byte[] value);

      abstract Builder setEncryptionKeyId(String value);
    }
  }
}