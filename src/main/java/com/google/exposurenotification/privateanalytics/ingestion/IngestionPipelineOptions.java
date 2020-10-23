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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Specific options for the pipeline.
 */
public interface IngestionPipelineOptions extends PipelineOptions {

  /**
   * Firebase project to read from.
   */
  @Description("Firebase Project Id to read from")
  @Required
  String getFirebaseProjectId();

  void setFirebaseProjectId(String value);

  /**
   * File prefix for output files for PHA
   */
  @Description("File prefix for output files for PHA.")
  @Required
  String getPHAOutput();

  void setPHAOutput(String value);

  /**
   * File prefix for output files for Facilitator
   */
  @Description("File prefix for output files for Facilitator.")
  @Required
  String getFacilitatorOutput();

  void setFacilitatorOutput(String value);

  /**
   * Start time of window to process. Used to filter documents that have been read from Firestore on
   * the "Creation" field.
   */
  @Description("Start time in seconds of documents to process")
  @Default.Long(0)
  Long getStartTime();

  void setStartTime(Long value);

  /**
   * Duration of time window to process. Used to filter documents that have been read from Firestore
   * on the "Creation" field.
   */
  @Description("Duration of window in seconds")
  @Default.Long(1728000000)
  Long getDuration();

  void setDuration(ValueProvider<Long> value);


  /**
   * Seconds to look before startTime when querying Firestore collection. Used to construct document
   * path for Firestore reads.
   */
  @Description(
      "Seconds to read backwards from startTime. Used to construct document path for Firestore"
          + " reads.")
  @Default.Long(3600)
  Long getGracePeriodBackwards();

  void setGracePeriodBackwards(Long value);

  /**
   * Seconds to look before startTime when querying Firestore. Used to construct document path for
   * Firestore reads.
   */
  @Description(
      "Seconds to read forward from startTime. Used to construct document path for Firestore"
          + " reads.")
  @Default.Long(3600)
  Long getGracePeriodForwards();

  void setGracePeriodForwards(Long value);

  /**
   * Minimum count of participants to preserve privacy(e.g., not allow batch of 1).
   */
  @Description("Minimum count of participants to preserve privacy.")
  @Default.Integer(0)
  Integer getMinimumParticipantCount();

  @Description("Minimum count of participants to preserve privacy.")
  void setMinimumParticipantCount(Integer value);

  /**
   * Whether to delete documents once they've been processed
   */
  @Description("Delete documents at end of pipeline")
  @Default.Boolean(false)
  Boolean getDelete();

  void setDelete(Boolean value);

  /**
   * Maximum number of query partitions to create for running Firestore read.
   */
  @Description("Maximum number of partitions to create for Firestore query.")
  @Default.Long(10000)
  Long getPartitionCount();

  void setPartitionCount(Long value);

  /**
   * Batch size of individual files.
   */
  @Description("Batch size of individual files.")
  @Default.Long(1000)
  Long getBatchSize();

  void setBatchSize(Long value);

  /**
   * Whether to check device hardware attestations
   */
  @Description("Verify device attestations")
  @Default.Boolean(true)
  Boolean getDeviceAttestation();

  void setDeviceAttestation(Boolean value);

  /**
   * Signing key resource name. See https://cloud.google.com/kms/docs/resource-hierarchy E.g.,
   * projects/$PROJECT_NAME/locations/global/keyRings/$RING/cryptoKeys/$KEY/cryptoKeyVersions/$VERSION
   */
  @Description("KMS resource name for signature generation")
  @Default.String("")
  String getKeyResourceName();

  void setKeyResourceName(String value);

  static String displayString(IngestionPipelineOptions options) {
    return "IngestionPipelineOptions:"
        + "\nfirebaseProjectId="
        + options.getFirebaseProjectId()
        + "\nstart="
        + options.getStartTime()
        + "\nduration="
        + options.getDuration()
        + "\nminParticipant="
        + options.getMinimumParticipantCount()
        + "\ndelete="
        + options.getDelete()
        + "\nkeyResourceName="
        + options.getKeyResourceName()
        + "\nphaOutput="
        + options.getPHAOutput()
        + "\nfacilitatorOutput="
        + options.getFacilitatorOutput();
  }
}
