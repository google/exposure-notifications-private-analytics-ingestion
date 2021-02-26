// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.exposurenotification.privateanalytics.ingestion.pipeline;

import com.google.exposurenotification.privateanalytics.ingestion.pipeline.FirestoreConnector.FirestoreDeleter;
import com.google.exposurenotification.privateanalytics.ingestion.pipeline.FirestoreConnector.FirestoreReader;
import java.time.Clock;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Pipeline to delete processed data shares from Firestore. */
public class DeletionPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DeletionPipeline.class);

  static PipelineResult runDeletionPipeline(IngestionPipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    long startTime =
        IngestionPipelineOptions.calculatePipelineStart(
            options.getStartTime(), options.getDuration(), 2, Clock.systemUTC());
    pipeline.apply(new FirestoreReader(startTime)).apply(new FirestoreDeleter());
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
      LOG.info("Metrics:\n\n{}", metrics);
    } catch (UnsupportedOperationException ignore) {
      // Known issue that this can throw when generating a template:
      // https://issues.apache.org/jira/browse/BEAM-9337
    } catch (Exception e) {
      LOG.error("Exception thrown during pipeline run.", e);
    }
  }
}
