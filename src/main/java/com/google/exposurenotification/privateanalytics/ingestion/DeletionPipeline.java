package com.google.exposurenotification.privateanalytics.ingestion;

import com.google.exposurenotification.privateanalytics.ingestion.FirestoreConnector.FirestoreDeleter;
import com.google.exposurenotification.privateanalytics.ingestion.FirestoreConnector.FirestoreReader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Pipeline to delete processed data shares from Firestore. */
public class DeletionPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(IngestionPipeline.class);

  static PipelineResult runDeletionPipeline(IngestionPipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(new FirestoreReader()).apply(new FirestoreDeleter());
    return pipeline.run();
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(IngestionPipelineOptions.class);
    IngestionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestionPipelineOptions.class);
    try {
      PipelineResult result = runDeletionPipeline(options);
      result.waitUntilFinish();
      MetricResults metrics = result.metrics();
      LOG.info("Metrics:\n\n" + metrics.toString());
    } catch (UnsupportedOperationException ignore) {
      // Known issue that this can throw when generating a template:
      // https://issues.apache.org/jira/browse/BEAM-9337
    } catch (Exception e) {
      LOG.error("Exception thrown during pipeline run.", e);
    }
  }
}
