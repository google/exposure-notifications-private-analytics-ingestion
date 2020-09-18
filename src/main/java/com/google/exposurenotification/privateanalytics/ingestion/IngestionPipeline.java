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

import com.google.exposurenotification.privateanalytics.ingestion.FirestoreConnector.FirestoreDeleter;
import com.google.exposurenotification.privateanalytics.ingestion.FirestoreConnector.FirestoreReader;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
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

  public static class SerializeDataShareFn extends DoFn<DataShare, List<PrioDataSharePacket>> {
    private static final Logger LOG = LoggerFactory.getLogger(SerializeDataShareFn.class);
    private final ValueProvider<Integer> numberOfServers;
    private final Counter dataShareIncluded = Metrics
            .counter(SerializeDataShareFn.class, "dataShareIncluded");
    private final Counter dataSharesWrongNumberServers = Metrics
            .counter(SerializeDataShareFn.class, "dataShareExcluded");

    public SerializeDataShareFn(ValueProvider<Integer> numberOfServers) {
      this.numberOfServers = numberOfServers;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

      List<Map<String, String>> encryptedDataShares = c.element().getEncryptedDataShares();
      if (encryptedDataShares.size() != numberOfServers.get()) {
        dataSharesWrongNumberServers.inc();
        LOG.trace("Excluded element: " + c.element());
        return;
      }

      List<PrioDataSharePacket> splitDataShares = new ArrayList<>();
      for (Map<String, String> dataShare : encryptedDataShares) {
        splitDataShares.add(
                PrioDataSharePacket.newBuilder()
                        .setEncryptionKeyId(dataShare.get(DataShare.ENCRYPTION_KEY_ID))
                        .setEncryptedPayload(
                                ByteBuffer.wrap(
                                        dataShare.get(DataShare.DATA_SHARE_PAYLOAD).getBytes()))
                        .setRPit(c.element().getRPit())
                        .setUuid(c.element().getUuid())
                        .build()
        );
      }
      dataShareIncluded.inc();
      c.output(splitDataShares);
    }
  }
  public static class ForkByIndexFn extends DoFn<List<PrioDataSharePacket>, PrioDataSharePacket> {
    private final int index;
    public ForkByIndexFn(int index) {
      this.index = index;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (index < c.element().size()) {
        c.output(c.element().get(index));
      } else {
        return;
      }
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
                    IngestionPipelineOptions options = c.getPipelineOptions()
                        .as(IngestionPipelineOptions.class);
                    LOG.info(IngestionPipelineOptions.displayString(options));
                  }
                }));

    // Ensure that 'numberOfServers' matches the number of output prefixes provided to 'output' option.
    ValueProvider<Integer> numberOfServers = options.getNumberOfServers();
    ValueProvider<List<String>> output = options.getOutput();
    PAssert.thatSingleton(pipeline.apply(Create.of(Arrays.asList("dummyPcollection"))))
            .satisfies(input -> {
              Assert.assertTrue("numberOfServers ("
                              + numberOfServers.get()
                              + ") does not match the number of file prefixes provided to the 'output' flag. "
                              + "output: "
                              + output.get(),
                      output.get().size() == numberOfServers.get());
              return null;
            });

    PCollection<DataShare> dataShares = pipeline.apply(new FirestoreReader());
    PCollection<List<PrioDataSharePacket>> serializedDataShares =
      processDataShares(dataShares, options)
          .apply("SerializeDataShares", ParDo.of(new SerializeDataShareFn(options.getNumberOfServers())));

    List<String> filePrefixes = options.getOutput().get();
    for (int i = 0; i < filePrefixes.size(); i++) {
      serializedDataShares
              .apply("ForkDataShares", ParDo.of(new ForkByIndexFn(i)))
              .apply(AvroIO.write(PrioDataSharePacket.class)
                      .to(filePrefixes.get(i))
                      .withSuffix(".avro"));
    }

    // TODO: use org.apache.beam.sdk.transforms.Wait to only delete when pipeline successfully writes batch files
    dataShares.apply(new FirestoreDeleter());

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
