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
import com.google.exposurenotification.privateanalytics.ingestion.SerializationFunctions.ForkByIndexFn;
import com.google.exposurenotification.privateanalytics.ingestion.SerializationFunctions.SerializeDataShareFn;
import com.google.exposurenotification.privateanalytics.ingestion.SerializationFunctions.SerializeIngestionHeaderFn;
import java.util.List;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.abetterinternet.prio.v1.PrioIngestionHeader;
import org.apache.beam.runners.core.construction.renderer.PipelineDotRenderer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.Values;
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

  // TODO: support arbitrary number of servers
  private static final int NUMBER_OF_SERVERS = 2;

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

  static PCollection<DataShare> processDataShares(
      PCollection<DataShare> inputDataShares, IngestionPipelineOptions options) {
    PCollection<DataShare> dataShares = inputDataShares
        .apply("FilterDates", ParDo.of(new DateFilterFn(options.getStartTime(),
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

    PCollection<DataShare> dataShares = pipeline.apply(new FirestoreReader());
    PCollection<String> headerFilenames = writeIngestionHeader(options, dataShares);
    headerFilenames.apply("GenerateSignatureFiles", ParDo.of(new SignatureKeyGeneration()));

    // TODO: Make separate batch UUID for each batch of data shares.
    PCollection<List<PrioDataSharePacket>> serializedDataShares =
      processDataShares(dataShares, options)
          .apply("SerializeDataShares", ParDo.of(new SerializeDataShareFn(NUMBER_OF_SERVERS)));
    writePrioDataSharePackets(options, serializedDataShares);

    // TODO: use org.apache.beam.sdk.transforms.Wait to only delete when pipeline successfully writes batch files
    dataShares.apply(new FirestoreDeleter());

    LOG.info("DOT graph representation:\n" + PipelineDotRenderer.toDotString(pipeline));
    pipeline.run().waitUntilFinish();
  }

  private static void writePrioDataSharePackets(IngestionPipelineOptions options,
      PCollection<List<PrioDataSharePacket>> serializedDataShares) {
      serializedDataShares
          .apply("ForkDataSharesForPHA", ParDo.of(new ForkByIndexFn(0)))
          .apply("WriteToPhaOutput", AvroIO.write(PrioDataSharePacket.class)
              .to(options.getPHAOutput())
              .withSuffix(".avro"));
      serializedDataShares
        .apply("ForkDataSharesForFacilitator", ParDo.of(new ForkByIndexFn(1)))
        .apply("WriteToFacilitatorOutput", AvroIO.write(PrioDataSharePacket.class)
          .to(options.getFacilitatorOutput())
          .withSuffix(".avro"));
  }

  private static PCollection<String> writeIngestionHeader(IngestionPipelineOptions options,
      PCollection<DataShare> dataShares) {
    return dataShares
        .apply(Sample.any(1))
        .apply("SerializeIngestionHeaders",
            ParDo.of(new SerializeIngestionHeaderFn(options.getStartTime(), options.getDuration())))
        .apply(AvroIO.write(PrioIngestionHeader.class)
            .to("ingestionHeader")
            .withSuffix(".avro").withOutputFilenames()).getPerDestinationOutputFilenames()
        .apply(Values.create());
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
