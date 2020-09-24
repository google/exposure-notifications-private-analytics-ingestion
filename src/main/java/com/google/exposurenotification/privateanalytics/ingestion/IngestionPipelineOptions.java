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

  /**
   * Comma-separated prefixes of where to write the output.
   * Note: Number of prefixes should equal number of servers.
   */
  @Description("List of prefixes of where to write the output files to")
  @Required
  ValueProvider<List<String>> getOutput();

  void setOutput(ValueProvider<List<String>> value);

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
   * Number of servers that the prio data shares will be split with.
   * Note: The number of servers must match the number of file prefixes specified in 'output' option.
   */
  @Description(
          "Number of servers that the prio data shares will be split with.")
  @Default.Integer(2)
  ValueProvider<Integer> getNumberOfServers();
  void setNumberOfServers(ValueProvider<Integer> value);
  static String displayString(IngestionPipelineOptions options){
    return "IngestionPipelineOptions:"
        + "\nfirebaseProjectId=" + options.getFirebaseProjectId()
        + "\nmetric=" + options.getMetric()
        + "\noutput=" + options.getOutput()
        + "\nstart=" + options.getStartTime()
        + "\nduration=" + options.getDuration()
        + "\nminParticipant=" + options.getMinimumParticipantCount()
        + "\ndelete=" + options.getDelete()
        + "\nnumberOfServers=" + options.getNumberOfServers();
  }
>>>>>>> f447de7 (log options during execution, graph during init)
}
