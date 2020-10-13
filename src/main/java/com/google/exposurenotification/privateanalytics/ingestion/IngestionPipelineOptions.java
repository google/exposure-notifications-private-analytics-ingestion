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
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Specific options for the pipeline.
 */
public interface IngestionPipelineOptions extends PipelineOptions {
  /**
   * Firebase project to read from.
   */
  @Description("Firebase Project Id to read from")
  @Required
  ValueProvider<String> getFirebaseProjectId();
  void setFirebaseProjectId(ValueProvider<String> value);

  /**
   * File prefix for output files for PHA
   */
  @Description("File prefix for output files for PHA.")
  @Required
  ValueProvider<String> getPHAOutput();
  void setPHAOutput(ValueProvider<String> value);

  /**
   * File prefix for output files for Facilitator
   */
  @Description("File prefix for output files for Facilitator.")
  @Required
  ValueProvider<String> getFacilitatorOutput();
  void setFacilitatorOutput(ValueProvider<String> value);

  /**
   * Start time of window to process
   */
  @Description(
      "Start time in seconds of documents to process")
  @Default.Long(0)
  ValueProvider<Long> getStartTime();

  void setStartTime(ValueProvider<Long> value);

  /**
   * Duration of time window to process
   */
  @Description(
      "Duration of window in seconds")
  @Default.Long(1728000000)
  ValueProvider<Long> getDuration();

  void setDuration(ValueProvider<Long> value);
<<<<<<< HEAD
=======

  /**
   * Window of time to read before startTime when querying Firestore.
   */
  @Description("Amount of time to read backwards from startTime")
  @Default.Long(3600)
  ValueProvider<Long> getGracePeriodBackwards();

  void setGracePeriodBackwards(ValueProvider<Long> value);

  /**
   * Window of time to read past startTime when querying Firestore.
   */
  @Description("Amount of time to read forward from startTime")
  @Default.Long(3600)
  ValueProvider<Long> getGracePeriodForwards();

  void setGracePeriodForwards(ValueProvider<Long> value);

  /**
   * Minimum count of participants to preserve privacy(e.g., not allow batch of 1).
   */
  @Description(
      "Minimum count of participants to preserve privacy.")
  @Default.Long(0)
  ValueProvider<Long> getMinimumParticipantCount();

  @Description(
          "Minimum count of participants to preserve privacy.")

  void setMinimumParticipantCount(ValueProvider<Long> value);

  /**
   * Whether to delete documents once they've been processed
   */
  @Description(
      "Delete documents at end of pipeline")
  @Default.Boolean(false)
  ValueProvider<Boolean> getDelete();

  void setDelete(ValueProvider<Boolean> value);

  /**
   * Maximum number of query partitions to create for running Firestore read.
   */
  @Description("Maximum number of partitions to create for Firestore query.")
  @Default.Long(10000)
  ValueProvider<Long> getPartitionCount();

  void setPartitionCount(ValueProvider<Long> value);

  /**
   * Whether to check device hardware attestations
   */
  @Description(
      "Verify device attestations")
  @Default.Boolean(true)
  ValueProvider<Boolean> getDeviceAttestation();

  void setDeviceAttestation(ValueProvider<Boolean> value);

  /**
   * Signing key resource name. See https://cloud.google.com/kms/docs/resource-hierarchy
   * E.g., projects/$PROJECT_NAME/locations/global/keyRings/$RING/cryptoKeys/$KEY/cryptoKeyVersions/$VERSION
   */
  @Description("KMS resource name for signature generation")
  @Default.String("")
  ValueProvider<String> getKeyResourceName();

  void setKeyResourceName(ValueProvider<String> value);

  static String displayString(IngestionPipelineOptions options){
    return "IngestionPipelineOptions:"
        + "\nfirebaseProjectId=" + options.getFirebaseProjectId()
        + "\nstart=" + options.getStartTime()
        + "\nduration=" + options.getDuration()
        + "\nminParticipant=" + options.getMinimumParticipantCount()
        + "\ndelete=" + options.getDelete()
        + "\nkeyResourceName=" + options.getKeyResourceName()
        + "\nphaOutput=" + options.getPHAOutput()
        + "\nfacilitatorOutput=" + options.getFacilitatorOutput();
  }
>>>>>>> f447de7 (log options during execution, graph during init)
}
