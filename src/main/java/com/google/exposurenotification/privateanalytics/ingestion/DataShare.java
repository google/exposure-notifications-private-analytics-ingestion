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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Pipeline view of Firestore documents corresponding to Prio data share pairs. */
@AutoValue
public abstract class DataShare implements Serializable {

  // Firestore document field names
  // TODO: link to ENX app github repo where these are defined
  public static final String PAYLOAD = "payload";

  // Payload fields
  public static final String ENCRYPTED_DATA_SHARES = "encryptedDataShares";
  public static final String CREATED = "created";
  public static final String UUID = "uuid";
  public static final String PRIO_PARAMS = "prioParams";

  // Prio Parameters field names
  public static final String PRIME = "prime";
  public static final String BINS = "bins";
  public static final String EPSILON = "epsilon";
  public static final String NUMBER_OF_SERVERS = "numberServers";
  public static final String HAMMING_WEIGHT = "hammingWeight"; // Optional field

  // Encrypted Data Share fields
  public static final String ENCRYPTION_KEY_ID = "encryptionKeyId";
  public static final String DATA_SHARE_PAYLOAD = "payload";

  /** Firestore document id */
  public abstract @Nullable String getId();
  public abstract @Nullable Long getCreated();
  public abstract @Nullable String getUuid();
  public abstract @Nullable Double getEpsilon();
  public abstract @Nullable Long getPrime();
  public abstract @Nullable Integer getBins();
  public abstract @Nullable Integer getNumberOfServers();
  public abstract @Nullable Long getRPit();
  public abstract @Nullable Integer getHammingWeight();
  public abstract @Nullable List<Map<String, String>> getEncryptedDataShares();

  // TODO: attestation, certificateChain, and signature

  /**
   * @return Pipeline projection of Firestore document
   */

  public static DataShare from(DocumentSnapshot doc) {
    DataShare.Builder builder = builder();
    try {
      builder.setId(doc.getId());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Missing required field: 'ID'", e);
    }

    Map<String, Object> payload = new HashMap<>();
    System.out.println(payload.getClass());
    try {
      payload = (Map<String, Object>) doc.get(PAYLOAD);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Missing required field: '" + PAYLOAD + "'", e);
    }
    builder.setCreated(checkThenGet(CREATED, Timestamp.class, payload, PAYLOAD).getSeconds());
    builder.setUuid(checkThenGet(UUID, String.class, payload, PAYLOAD));

    Map<String, Object> prioParams = checkThenGet(PRIO_PARAMS, HashMap.class, payload, PAYLOAD);
    Long prime = checkThenGet(PRIME, Long.class, prioParams, PRIO_PARAMS);
    builder.setPrime(prime);
    builder.setRPit(ThreadLocalRandom.current().nextLong(prime));
    builder.setEpsilon(checkThenGet(EPSILON, Double.class, prioParams, PRIO_PARAMS));
    builder.setBins(checkThenGet(BINS, Long.class, prioParams, PRIO_PARAMS).intValue());
    int numberOfServers = checkThenGet(NUMBER_OF_SERVERS, Long.class, prioParams, PRIO_PARAMS).intValue();
    builder.setNumberOfServers(numberOfServers);
    if (prioParams.get(HAMMING_WEIGHT) != null) {
      builder.setHammingWeight(checkThenGet(HAMMING_WEIGHT, Long.class, prioParams, PRIO_PARAMS).intValue());
    }

    List<Map<String, String>> encryptedDataShares =
            checkThenGet(ENCRYPTED_DATA_SHARES, ArrayList.class, payload, PAYLOAD);
    if (encryptedDataShares.size() != numberOfServers) {
      throw new IllegalArgumentException(
              "Mismatch between number of servers (" + numberOfServers + ") and number of data shares (" + encryptedDataShares.size() + ")");
    }

    // Ensure data shares are of correct type.
    for (int i = 0; i < encryptedDataShares.size(); i++) {
      checkThenGet(ENCRYPTION_KEY_ID, String.class, encryptedDataShares.get(i), ENCRYPTED_DATA_SHARES + "[" +  i + "]");
      checkThenGet(DATA_SHARE_PAYLOAD, String.class, encryptedDataShares.get(i), ENCRYPTED_DATA_SHARES + "[" +  i + "]");
    }
    builder.setEncryptedDataShares(encryptedDataShares);

    return builder.build();
  }


  static Builder builder() {
    return new AutoValue_DataShare.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract DataShare build();
    abstract Builder setId(@Nullable String value);
    abstract Builder setCreated(@Nullable Long value);
    abstract Builder setUuid(@Nullable String value);
    abstract Builder setEpsilon(@Nullable Double value);
    abstract Builder setPrime(@Nullable Long value);
    abstract Builder setBins(@Nullable Integer value);
    abstract Builder setNumberOfServers(@Nullable Integer value);
    abstract Builder setRPit(@Nullable Long value);
    abstract Builder setHammingWeight(@Nullable Integer value);
    abstract Builder setEncryptedDataShares(@Nullable List<Map<String, String>> value);
  }

  // Returns a casted element from a map and provides detailed exceptions upon failure.
  private static <T, E> T checkThenGet(String field, Class<T> fieldClass, Map<String, E> sourceMap, String sourceName) {
    String errorMessage = "Missing required field: '" + field + "' from '" + sourceName + "'";
    if (!sourceMap.containsKey(field) || sourceMap.get(field) == null) {
      throw new IllegalArgumentException("Missing required field: '" + field + "' from '" + sourceName + "'");
    }

    try {
      return fieldClass.cast(sourceMap.get(field));
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Error casting '" + field + "' from '" + sourceName + "' to " + fieldClass.getName(), e);
    }
  }
}
