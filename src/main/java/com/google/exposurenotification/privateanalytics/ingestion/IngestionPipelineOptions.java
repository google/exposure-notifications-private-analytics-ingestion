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

import java.util.List;
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
   * Path to the service account key json file.
   */
  @Description("Path to the service account key json file")
  @Required
  ValueProvider<String> getServiceAccountKey();

  void setServiceAccountKey(ValueProvider<String> value);

  /**
   * Firebase project to read from.
   */
  @Description("Firebase Project Id")
  @Required

  ValueProvider<String> getFirebaseProjectId();

  void setFirebaseProjectId(ValueProvider<String> value);

  /**
   * Metric to aggregate
   */
  @Description("Metric to aggregate")
  @Required
  ValueProvider<String> getMetric();

  void setMetric(ValueProvider<String> value);

  @Description("File prefix of where to write the output files to for PHA.")
  @Required
  ValueProvider<String> getPHAOutput();
  void setPHAOutput(ValueProvider<String> value);

  @Description("File prefix of where to write the output files to for Facilitator.")
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
  @Default.Long(172800000)
  ValueProvider<Long> getDuration();

  void setDuration(ValueProvider<Long> value);
<<<<<<< HEAD
=======

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
   * ProjectID for signature generation
   */
  @Description("Project ID for signature generation")
  @Default.String("")
  ValueProvider<String> getProjectId();
  void setProjectId(ValueProvider<String> value);

  /**
   * Location id
   */
  @Description("Location ID")
  @Default.String("")
  ValueProvider<String> getLocationId();
  void setLocationId(ValueProvider<String> value);

  /**
   * Key Ring id
   */
  @Description("Key Ring ID")
  @Default.String("")
  ValueProvider<String> getKeyRingId();
  void setKeyRingId(ValueProvider<String> value);

  /**
   * Key id
   */
  @Description("Key ID")
  @Default.String("")
  ValueProvider<String> getKeyId();
  void setKeyId(ValueProvider<String> value);

  /**
   * Key Version id
   */
  @Description("Key Version ID")
  @Default.String("")
  ValueProvider<String> getKeyVersionId();
  void setKeyVersionId(ValueProvider<String> value);


  static String displayString(IngestionPipelineOptions options){
    return "IngestionPipelineOptions:"
        + "\nfirebaseProjectId=" + options.getFirebaseProjectId()
        + "\nmetric=" + options.getMetric()
        + "\nstart=" + options.getStartTime()
        + "\nduration=" + options.getDuration()
        + "\nminParticipant=" + options.getMinimumParticipantCount()
        + "\ndelete=" + options.getDelete()
        + "\nprojectId=" + options.getProjectId()
        + "\nlocationId=" + options.getLocationId()
        + "\nkeyRingId=" + options.getKeyRingId()
        + "\nkeyId=" + options.getKeyId()
        + "\nKeyVersionId=" + options.getKeyVersionId()
        + "\nphaOutput=" + options.getPHAOutput()
        + "\nfacilitatorOutput=" + options.getFacilitatorOutput();
  }
>>>>>>> f447de7 (log options during execution, graph during init)
}
