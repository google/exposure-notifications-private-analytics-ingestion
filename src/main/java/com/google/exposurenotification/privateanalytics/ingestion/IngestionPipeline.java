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

import org.abetterinternet.prio.v1.PrioDataSharePacket;
import java.nio.ByteBuffer;
import org.apache.beam.runners.core.construction.renderer.PipelineDotRenderer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
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

  public static class SerializeDataShareFn extends SimpleFunction<DataShare, PrioDataSharePacket> {

    @Override
    public PrioDataSharePacket apply(DataShare input) {
      return PrioDataSharePacket.newBuilder()
              .setEncryptionKeyId("hardCodedID")
              .setRPit(input.getRPit())
              .setUuid(input.getUuid())
              .setEncryptedPayload(ByteBuffer.wrap(new byte[] {0x01, 0x02, 0x03, 0x04, 0x05}))
              .build();
    }
  }

  /**
   * A DoFn that filters documents in particular time window
   */
  public static class DateFilterFn extends DoFn<DataShare, DataShare> {

    private static final Logger LOG = LoggerFactory.getLogger(DateFilterFn.class);

    private final Counter dateFilterIncluded = Metrics
        .counter(DateFilterFn.class, "dateFilterIncluded");
    private final Counter dateFilterExcluded = Metrics
        .counter(DateFilterFn.class, "dateFilterExcluded");
    private final ValueProvider<Long> startTime;
    private final ValueProvider<Long> duration;

    public DateFilterFn(ValueProvider<Long> startTime, ValueProvider<Long> duration) {
      this.startTime = startTime;
      this.duration = duration;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element().getCreated() == null || c.element().getCreated() == 0) {
        // TODO: fork these documents off somewhere so that they are still deleted downstream and we don't accumulate junk
        return;
      }
      if (c.element().getCreated() >= startTime.get() &&
          c.element().getCreated() < startTime.get() + duration.get()) {
        LOG.debug("Included: " + c.element());
        dateFilterIncluded.inc();
        c.output(c.element());
      } else {
        LOG.trace("Excluded: " + c.element());
        dateFilterExcluded.inc();
      }
    }
  }

<<<<<<< HEAD
  static void runIngestionPipeline(IngestionPipelineOptions options) throws Exception {
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(new FirestoreReader())
        .apply("Filter dates",
            ParDo.of(new DateFilterFn(options.getStartTime(), options.getDuration())))
        // TODO: fork data shares for PHA and Facilitator
        // TODO(guray): bail if not enough data shares to ensure min-k anonymity:
        //  https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/transforms/Count.html#globally--
        // TODO(justinowusu): s/TextIO/AvroIO/
        //  https://beam.apache.org/releases/javadoc/2.4.0/org/apache/beam/sdk/io/AvroIO.html
        .apply("SerializeElements", MapElements.via(new FormatAsTextFn()))
        .apply("WriteBatches", TextIO.write().to(options.getOutput()));

=======
  /**
   * Process input {@link PCollection<DataShare>}, and make them available for final serialization.
   */
  static PCollection<DataShare> processDataShares(
      PCollection<DataShare> inputDataShares, IngestionPipelineOptions options) {
    PCollection<DataShare> dataShares = inputDataShares
        .apply("Filter dates", ParDo.of(new DateFilterFn(options.getStartTime(),
            options.getDuration())))
        .apply(new DeviceAttestation());
    // TODO: fork data shares for PHA and Facilitator

    ValueProvider<Long> minParticipantCount = options.getMinimumParticipantCount();
    PAssert.thatSingleton(dataShares.apply("CountParticipants", Count.globally()))
        .satisfies(input -> {
          Assert.assertTrue("Number of participating devices is:"
                  + input
                  + " which is less than the minimum requirement of "
                  + minParticipantCount.get(),
              input >= minParticipantCount.get());
          return null;
        });

    return dataShares;
  }


  static void runIngestionPipeline(IngestionPipelineOptions options) {
    Pipeline pipeline =  Pipeline.create(options);
    // Log pipeline options.
    // On Cloud Dataflow options will be specified on templated job, so we need to retrieve from
    // the ValueProviders as part of job execution and not during setup.
    pipeline.apply(Create.of(1))
        .apply(
            ParDo.of(
                new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    IngestionPipelineOptions options = c.getPipelineOptions().as(IngestionPipelineOptions.class);
                    LOG.info(IngestionPipelineOptions.displayString(options));
                  }
                }));

    processDataShares(pipeline.apply(new FirestoreReader()), options)
        .apply("SerializeDataShares", MapElements.via(new SerializeDataShareFn()))
        .apply(AvroIO.write(PrioDataSharePacket.class)
            .to(options.getOutput())
            .withSuffix(".avro"));
    LOG.info("DOT graph representation:\n" + PipelineDotRenderer.toDotString(pipeline));
>>>>>>> f447de7 (log options during execution, graph during init)
    pipeline.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(IngestionPipelineOptions.class);
    IngestionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestionPipelineOptions.class);

    try {
      runIngestionPipeline(options);
    } catch (UnsupportedOperationException ignore) {
      // Known issue that this can throw when generating a template:
      // https://issues.apache.org/jira/browse/BEAM-9337
    } catch (Exception e) {
      LOG.error("Exception thrown during pipeline run.", e);
    }
  }
}
