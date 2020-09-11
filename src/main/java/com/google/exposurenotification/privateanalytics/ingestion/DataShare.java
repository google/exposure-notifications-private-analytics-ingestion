package com.google.exposurenotification.privateanalytics.ingestion;

import com.google.auto.value.AutoValue;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Pipeline view of Firestore documents corresponding to Prio data share pairs. */
@AutoValue
public abstract class DataShare implements Serializable {

  // Firestore document field names
  public static final String CREATED = "created";

  /** Firestore document id */
  public abstract @Nullable String getId();
  public abstract @Nullable Long getCreated();

  // TODO: List<encrypted payload>, uuid, attestation, etc

  /**
   * @return Pipeline projection of Firestore document
   */
  public static DataShare from(DocumentSnapshot doc) {
    DataShare.Builder builder = builder();
    if (doc.getId() != null) {
      builder.setId(doc.getId());
    }
    if (doc.get(CREATED) != null) {
      try {
        Timestamp timestamp = (Timestamp) doc.get(CREATED);
        builder.setCreated(timestamp.getSeconds());
      } catch (ClassCastException ignore) {
        // These will be filtered out elsewhere in the pipeline
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
  }
}
