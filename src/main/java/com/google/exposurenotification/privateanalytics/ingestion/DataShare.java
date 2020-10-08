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
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Pipeline view of Firestore documents corresponding to Prio data share pairs.
 */
@AutoValue
public abstract class DataShare implements Serializable {

  private static final long serialVersionUID = 1L;

  // Firestore document field names. See
  // https://github.com/google/exposure-notifications-android/tree/master/app/src/main/java/com/google/android/apps/exposurenotification/privateanalytics/PrivateAnalyticsFirestoreRepository.java#50
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
  public static final String NUMBER_OF_SERVERS = "numberServers";
  public static final String HAMMING_WEIGHT = "hammingWeight";

  // Encrypted Data Share fields
  public static final String ENCRYPTION_KEY_ID = "encryptionKeyId";
  public static final String DATA_SHARE_PAYLOAD = "payload";

  /** Firestore document path */
  public abstract @Nullable String getPath();

  public abstract @Nullable Long getCreated();

  public abstract @Nullable String getUuid();

  public abstract @Nullable String getException();

  public abstract @Nullable Long getRPit();

  public abstract @Nullable List<Map<String, String>> getEncryptedDataShares();

  public abstract @Nullable DataShareMetadata getDataShareMetadata();


  /**
   * @return Pipeline projection of Firestore document
   */
  public static DataShare from(DocumentSnapshot doc) {
    DataShare.Builder builder = builder();
    try {
      builder.setPath(doc.getReference().getPath());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Missing required field: Path", e);
    }

    // Step 1: Process the payload.
    Map<String, Object> payload = (Map<String, Object>) doc.get(PAYLOAD);
    if (payload == null) {
      throw new IllegalArgumentException("Missing required field: " + PAYLOAD);
    }

    builder.setCreated(checkThenGet(CREATED, Timestamp.class, payload, PAYLOAD).getSeconds());
    builder.setUuid(checkThenGet(UUID, String.class, payload, PAYLOAD));

    // Get the Prio parameters.
    Map<String, Object> prioParams = checkThenGet(PRIO_PARAMS, Map.class, payload, PAYLOAD);
    Long prime = checkThenGet(PRIME, Long.class, prioParams, PRIO_PARAMS);

    DataShareMetadata.Builder metadataBuilder = DataShareMetadata.builder();
    metadataBuilder.setPrime(prime);
    metadataBuilder.setEpsilon(checkThenGet(EPSILON, Double.class, prioParams, PRIO_PARAMS));
    metadataBuilder.setBins(checkThenGet(BINS, Long.class, prioParams, PRIO_PARAMS).intValue());
    int numberOfServers = checkThenGet(NUMBER_OF_SERVERS, Long.class, prioParams, PRIO_PARAMS)
        .intValue();
    metadataBuilder.setNumberOfServers(numberOfServers);
    if (prioParams.get(HAMMING_WEIGHT) != null) {
      metadataBuilder.setHammingWeight(
          checkThenGet(HAMMING_WEIGHT, Long.class, prioParams, PRIO_PARAMS).intValue());
    }

    builder.setDataShareMetadata(metadataBuilder.build());

    // Generate a r_PIT randomly for every data share.
    Long rPit = generateRandom(prime);
    builder.setRPit(rPit);

    // Get the encrypted shares.
    List<Map<String, String>> encryptedDataShares = checkThenGet(ENCRYPTED_DATA_SHARES,
        ArrayList.class, payload,
        PAYLOAD);
    if (encryptedDataShares.size() != numberOfServers) {
      throw new IllegalArgumentException("Mismatch between number of servers (" + numberOfServers
          + ") and number of data shares (" + encryptedDataShares.size() + ")");
    }

    // Ensure data shares are of correct type.
    for (int i = 0; i < encryptedDataShares.size(); i++) {
      checkThenGet(ENCRYPTION_KEY_ID, String.class, encryptedDataShares.get(i),
          ENCRYPTED_DATA_SHARES + "[" + i + "]");
      checkThenGet(DATA_SHARE_PAYLOAD, String.class, encryptedDataShares.get(i),
          ENCRYPTED_DATA_SHARES + "[" + i + "]");
    }
    builder.setEncryptedDataShares(encryptedDataShares);

    // Step 2: Get the exception message (if it's present)
    if (doc.contains(EXCEPTION) && doc.get(EXCEPTION) != null) {
      try {
        builder.setException(String.class.cast(doc.get(EXCEPTION)));
        // If the exception is present, no need to check for signature or cerficates
        return builder.build();
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            "Error casting 'exception' from 'payload' to exception", e);
      }
    }

    // Step 3: Get the signature.
    String signature = (String) doc.get(SIGNATURE);
    if (signature == null) {
      throw new IllegalArgumentException("Missing required field: " + SIGNATURE);
    }
    builder.setSignature(signature);

    // Step 4: Get the chain of X509 certificates.
    List<String> certChainString = (List<String>) doc.get(CERT_CHAIN);
    if (certChainString == null) {
      throw new IllegalArgumentException("Missing required field: " + CERT_CHAIN);
    }

    List<X509Certificate> certChainX509 = new ArrayList<>();
    try {
      CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
      for (String certString : certChainString) {
        byte[] cert = Base64.getDecoder().decode(certString);
        // Parse as X509 certificate.
        X509Certificate certX509 = (X509Certificate) certFactory
            .generateCertificate(new ByteArrayInputStream(cert));
        certChainX509.add(certX509);
      }
    } catch (Exception e) {
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
  abstract static class Builder {
    abstract DataShare build();

    abstract Builder setPath(@Nullable String value);

    abstract Builder setCreated(@Nullable Long value);

    abstract Builder setUuid(@Nullable String value);

    abstract Builder setException(@Nullable String value);

    abstract Builder setRPit(@Nullable Long value);

    abstract Builder setEncryptedDataShares(@Nullable List<Map<String, String>> value);

    abstract Builder setDataShareMetadata(@Nullable DataShareMetadata value);

    abstract Builder setSignature(@Nullable String value);

    abstract Builder setCertificateChain(@Nullable List<X509Certificate> certChain);
  }

  // Returns a casted element from a map and provides detailed exceptions upon
  // failure.
  private static <T, E> T checkThenGet(String field, Class<T> fieldClass, Map<String, E> sourceMap,
      String sourceName) {
    if (!sourceMap.containsKey(field) || sourceMap.get(field) == null) {
      throw new IllegalArgumentException(
          "Missing required field: '" + field + "' from '" + sourceName + "'");
    }

    try {
      return fieldClass.cast(sourceMap.get(field));
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          "Error casting '" + field + "' from '" + sourceName + "' to " + fieldClass.getName(), e);
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
}

@AutoValue
abstract class DataShareMetadata implements Serializable {
  private static final long serialVersionUID = 1L;

  public abstract @Nullable Double getEpsilon();

  public abstract @Nullable Long getPrime();

  public abstract @Nullable Integer getBins();

  public abstract @Nullable Integer getNumberOfServers();

  public abstract @Nullable Integer getHammingWeight();

  static DataShareMetadata.Builder builder() {
    return new AutoValue_DataShareMetadata.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract DataShareMetadata build();

    abstract Builder setEpsilon(@Nullable Double value);

    abstract Builder setPrime(@Nullable Long value);

    abstract Builder setBins(@Nullable Integer value);

    abstract Builder setNumberOfServers(@Nullable Integer value);

    abstract Builder setHammingWeight(@Nullable Integer value);
  }
}