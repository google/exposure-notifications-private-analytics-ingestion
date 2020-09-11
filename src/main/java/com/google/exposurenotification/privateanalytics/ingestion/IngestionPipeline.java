/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.exposurenotification.privateanalytics.ingestion;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline to export Exposure Notification Private Analytics data shares from Firestore and
 * translate into format usable by downstream batch processing by Health Authorities and
 * Facilitators.*
 *
 * <p>To execute this pipeline locally, specify general pipeline configuration:
 *
 * <pre>{@code
 * --project=YOUR_PROJECT_ID
 * }</pre>
 *
 * <p>To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 */
public class IngestionPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(IngestionPipeline.class);

  /**
   * A Temporary SimpleFunction that converts a DataShare into a printable string.
   */
  public static class FormatAsTextFn extends SimpleFunction<DataShare, String> {

    @Override
    public String apply(DataShare input) {
      return input.toString();
    }
  }

  /**
   * A DoFn that filters documents not in the date range
   */
  public static class DateFilterFn extends DoFn<DataShare, DataShare> {

    private static final Logger LOG = LoggerFactory.getLogger(DateFilterFn.class);

    private final Counter dateFilterIncluded = Metrics
        .counter(DateFilterFn.class, "dateFilterIncluded");
    private final Counter dateFilterExcluded = Metrics
        .counter(DateFilterFn.class, "dateFilterExcluded");
    private final ValueProvider<Long> startTime;

    public DateFilterFn(ValueProvider<Long> startTime) {
      this.startTime = startTime;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element().getCreated() > startTime.get()) {
        LOG.debug("Included: " + c.element());
        dateFilterIncluded.inc();
        c.output(c.element());
      } else {
        LOG.trace("Excluded: " + c.element());
        dateFilterExcluded.inc();
      }
    }
  }

  static void runIngestionPipeline(IngestionPipelineOptions options) throws Exception {
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(new FirestoreReader())
        .apply(ParDo.of(new DateFilterFn(options.getStartTime())))
        // TODO(guray): bail if not enough data shares to ensure min-k anonymity:
        // https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/transforms/Count.html#globally--
        .apply(MapElements.via(new FormatAsTextFn()))
        // TODO(justinowusu): s/TextIO/AvroIO/
        // https://beam.apache.org/releases/javadoc/2.4.0/org/apache/beam/sdk/io/AvroIO.html
        .apply("WriteCounts", TextIO.write().to(options.getOutput()));

    pipeline.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(IngestionPipelineOptions.class);
    IngestionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestionPipelineOptions.class);

    try {
      runIngestionPipeline(options);
    } catch (Exception e) {
      if (e instanceof UnsupportedOperationException) {
        // Apparently a known issue that this throws when generating a template:
        // https://issues.apache.org/jira/browse/BEAM-9337
      } else {
        LOG.error("Exception thrown during pipeline run.", e);
      }
    }
  }
}
