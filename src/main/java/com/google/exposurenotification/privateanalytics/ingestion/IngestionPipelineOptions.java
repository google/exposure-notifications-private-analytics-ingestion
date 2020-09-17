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

  /**
   * Where to write the output.
   */
  @Description("Prefix of the output files to write to")
  @Required
  ValueProvider<String> getOutput();

  void setOutput(ValueProvider<String> value);

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

  void setMinimumParticipantCount(ValueProvider<Long> value);

  static String displayString(IngestionPipelineOptions options){
    return "IngestionPipelineOptions:"
        + "\nfirebaseProjectId=" + options.getFirebaseProjectId()
        + "\nmetric=" + options.getMetric()
        + "\noutput=" + options.getOutput()
        + "\nstart=" + options.getStartTime()
        + "\nduration=" + options.getDuration()
        + "\nminParticipant=" + options.getMinimumParticipantCount();
  }
>>>>>>> f447de7 (log options during execution, graph during init)
}
