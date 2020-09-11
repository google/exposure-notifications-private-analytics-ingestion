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
}
