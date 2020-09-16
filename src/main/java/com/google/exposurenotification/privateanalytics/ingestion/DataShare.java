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
import java.util.HashMap;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Pipeline view of Firestore documents corresponding to Prio data share pairs. */
@AutoValue
public abstract class DataShare implements Serializable {

  // Firestore document field names
  // TODO: link to ENX app github repo where these are defined
  public static final String CREATED = "created";
  public static final String UUID = "uuid";
  public static final String PRIO_PARAMS = "prioParams";
  public static final String R_PIT = "rPit";

  // Prio Parameters field names
  public static final String PRIME = "prime";
  public static final String BINS = "bins";
  public static final String EPSILON = "epsilon";
  public static final String NUMBER_OF_SERVERS = "numberServers";
  public static final String HAMMING_WEIGHT = "hammingWeight"; // Optional field
  public static final String[] requiredPrioParams = new String[] {PRIME, BINS, EPSILON, NUMBER_OF_SERVERS};

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


  // TODO: List<encrypted payload>, attestation, etc

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

    try {
      Timestamp timestamp = doc.getTimestamp(CREATED);
        builder.setCreated(timestamp.getSeconds());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Missing required field: '" + CREATED, e);
    }

    try {
      builder.setUuid(doc.getString(UUID));
    } catch (NullPointerException e) {
      throw new IllegalArgumentException("Missing required field: '" + UUID, e);
    }

    try {
      builder.setRPit(doc.getLong(R_PIT));
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Missing required field: " + R_PIT, e);
    }

    Map<String, Object> prioParams = new HashMap<>();
    try {
      prioParams = (Map<String, Object>) doc.get(PRIO_PARAMS);
      if (prioParams == null) {
        throw new IllegalArgumentException("Missing required field: '" + PRIO_PARAMS + "'");
      }
    } catch (RuntimeException e) {
        throw new IllegalArgumentException("Missing required field: '" + PRIO_PARAMS + "'");
      }

    for (String key : requiredPrioParams) {
      if (prioParams.get(key) == null) {
          throw new IllegalArgumentException("Missing required field: '" + key + "' from '" + PRIO_PARAMS + "'");
      }
    }

    for (String key : requiredPrioParams) {
      if (prioParams.get(key) == null) {
        throw new IllegalArgumentException("Missing required field: " + key + " from '" + PRIO_PARAMS + "'");
      }
    }

    try {
    builder.setPrime((Long) prioParams.get(PRIME));
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Missing required field: '" + PRIME + "' from '" + PRIO_PARAMS + "'", e);
    }

    try {
      builder.setEpsilon( (Double) prioParams.get(EPSILON));
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Missing required field: '" + EPSILON + "' from '" + PRIO_PARAMS + "'", e);
    }

    try {
      builder.setBins(( (Long) prioParams.get(BINS)).intValue());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Missing required field: '" + BINS + "' from '" + PRIO_PARAMS + "'", e);
    }

    try {
      builder.setNumberOfServers(( (Long) prioParams.get(NUMBER_OF_SERVERS)).intValue());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Missing required field: '" + NUMBER_OF_SERVERS + "' from '" + PRIO_PARAMS + "'", e);
    }

    if (prioParams.get(HAMMING_WEIGHT) != null) {
      try {
        builder.setHammingWeight(((Long) prioParams.get(HAMMING_WEIGHT)).intValue());
      } catch (RuntimeException e) {
        throw new IllegalArgumentException("Error parsing field: '" + HAMMING_WEIGHT + "' from '" + PRIO_PARAMS + "'", e);
      }
    }

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
  }
}
