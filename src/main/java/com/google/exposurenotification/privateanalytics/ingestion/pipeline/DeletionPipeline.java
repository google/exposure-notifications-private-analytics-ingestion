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

package com.google.exposurenotification.privateanalytics.ingestion.pipeline;

import com.google.exposurenotification.privateanalytics.ingestion.pipeline.FirestoreConnector.FirestorePartitionQueryCreation;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.Write;
import java.time.Clock;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosOptions;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Pipeline to delete processed data shares from Firestore. */
public class DeletionPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DeletionPipeline.class);

  static void buildDeletionPipeline(IngestionPipelineOptions options, Pipeline pipeline) {
    DataflowPipelineOptions dataflowPipelineOptions = options.as(DataflowPipelineOptions.class);
    RpcQosOptions.Builder rpcQosOptionsBuilder = RpcQosOptions.newBuilder();
    int maxNumWorkers = dataflowPipelineOptions.getMaxNumWorkers();
    if (maxNumWorkers > 0) {
      rpcQosOptionsBuilder.withHintMaxNumWorkers(maxNumWorkers);
    }
    long startTime =
        IngestionPipelineOptions.calculatePipelineStart(
            options.getStartTime(), options.getDuration(), 2, Clock.systemUTC());
    pipeline
        .apply(new FirestorePartitionQueryCreation(startTime))
        .apply(FirestoreIO.v1().read().partitionQuery().withNameOnlyQuery().build())
        .apply(FirestoreIO.v1().read().runQuery().build())
        .apply(FirestoreConnector.filterRunQueryResponseHasDocument())
        .apply(
            MapElements.via(
                new SimpleFunction<RunQueryResponse, Write>() {
                  @Override
                  public Write apply(RunQueryResponse input) {
                    return Write.newBuilder().setDelete(input.getDocument().getName()).build();
                  }
                }))
        .apply(
            FirestoreIO.v1()
                .write()
                .batchWrite()
                .withRpcQosOptions(rpcQosOptionsBuilder.build())
                .build());
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(IngestionPipelineOptions.class);
    IngestionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestionPipelineOptions.class);
    try {
      Pipeline pipeline = Pipeline.create(options);
      buildDeletionPipeline(options, pipeline);
      PipelineResult result = pipeline.run();
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
