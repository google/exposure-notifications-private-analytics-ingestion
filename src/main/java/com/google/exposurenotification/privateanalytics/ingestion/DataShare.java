package com.google.exposurenotification.privateanalytics.ingestion;

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

/** POJO for the collection type processed in this pipeline */
@DefaultSchema(JavaBeanSchema.class)
public class DataShare implements Serializable {

  // Firestore document field names
  public static final String PAYLOAD = "payload";
  public static final String ENCRYPTION_KEY_ID = "encryptionKeyId";
  public static final String EPSILON = "epsilon";
  public static final String PRIME = "prime";
  public static final String NUMBER_SERVERS = "numberServers";
  public static final String CREATED = "created";
  public static final String PRIO_PARAMS = "prioParams";
  public static final String DEVICE_ATTESTATION = "deviceAttestation";
  public static final String ENCRYPTED_DATA_SHARES = "encryptedDataShares";

  private @Nullable String id;
  private long created;

  public DataShare() {}

  public DataShare(@Nullable String id, long created) {
    this.id = id;
    this.created = created;
  }

  @Nullable
  public String getId() {
    return id;
  }

  public void setId(@Nullable String id) {
    this.id = id;
  }

  public long getCreated() {
    return created;
  }

  public void setCreated(long created) {
    this.created = created;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataShare dataShare = (DataShare) o;
    return created == dataShare.created &&
        Objects.equals(id, dataShare.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, created);
  }

  @Override
  public String toString() {
    return "DataShare{" +
        "id='" + id + '\'' +
        ", created=" + created +
        '}';
  }
}
