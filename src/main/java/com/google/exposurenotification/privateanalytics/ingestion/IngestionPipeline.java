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

import com.google.exposurenotification.privateanalytics.ingestion.DataShare.ConstructDataSharesFn;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.DataShareMetadata;
import com.google.exposurenotification.privateanalytics.ingestion.FirestoreConnector.FirestoreReader;
import com.google.firestore.v1.Document;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
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
      PCollection<DataShare> inputDataShares, IngestionPipelineOptions options, String metric) {
    PCollection<DataShare> dataShares = inputDataShares
        .apply("FilterDates_" + metric, ParDo.of(new DateFilterFn(options.getStartTime(),
            options.getDuration(), metric)))
        .apply("DeviceAttestation_" + metric, new DeviceAttestation());
    ValueProvider<Long> minParticipantCount = options.getMinimumParticipantCount();
    PAssert.thatSingleton(dataShares.apply("CountParticipants_" + metric, Count.globally()))
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

  /** Perform the input, processing and output for the full ingestion pipeline. */
  static PipelineResult runIngestionPipeline(IngestionPipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    long startTime =
        IngestionPipelineOptions.calculatePipelineStart(
            options.getStartTime(), options.getDuration(), 1, Clock.systemUTC());
    PCollection<DataShare> dataShares =
        pipeline
            .apply(new FirestoreReader(startTime))
            // Ensure distinctness of data shares based on document path
            .apply(
                Distinct.<Document, String>withRepresentativeValueFn(
                    // Not using a lambda here as Beam has trouble inferring a coder
                    new SerializableFunction<Document, String>() {
                      @Override
                      public String apply(Document document) {
                        return document.getName();
                      }
                    }))
            .apply(ParDo.of(new ConstructDataSharesFn()));
    processDataShares(dataShares).apply("SerializePacketHeaderSig", ParDo.of(new BatchWriterFn()));
    return pipeline.run();
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(IngestionPipelineOptions.class);
    IngestionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestionPipelineOptions.class);

    readOptionsFromManifests(options);

    try {
      PipelineResult result = runIngestionPipeline(options);
      result.waitUntilFinish();
      MetricResults metrics = result.metrics();
      LOG.info("Metrics:\n\n{}", metrics.allMetrics().getCounters());
      metrics
          .allMetrics()
          .getDistributions()
          .forEach(
              distribution -> {
                LOG.info(
                    "Distribution:  {}: DistributionResult{sum={}, count={}, min={}, max={}, mean={}}",
                    distribution.getName(),
                    distribution.getCommitted().getSum(),
                    distribution.getCommitted().getCount(),
                    distribution.getCommitted().getMin(),
                    distribution.getCommitted().getMax(),
                    distribution.getCommitted().getMean());
              });
    } catch (UnsupportedOperationException ignore) {
      // Known issue that this can throw when generating a template:
      // https://issues.apache.org/jira/browse/BEAM-9337
    } catch (Exception e) {
      LOG.error("Exception thrown during pipeline run.", e);
    }
  }

  private static void readOptionsFromManifests(IngestionPipelineOptions options) {
    if (!"".equals(options.getPhaManifestURL())) {
      DataProcessorManifest manifestPha = new DataProcessorManifest(options.getPhaManifestURL());

      if (manifestPha.isAwsBucket()) {
        options.setPhaAwsBucketRegion(manifestPha.getAwsBucketRegion());
        options.setPhaAwsBucketName(manifestPha.getAwsBucketName());
        options.setPhaAwsBucketRole(manifestPha.getAwsRole());
      }
      options.setPhaOutput(getOutputPrefix(options.getPhaOutput(), manifestPha));
    }

    if (!"".equals(options.getFacilitatorManifestURL())) {
      DataProcessorManifest manifestFacilitator =
          new DataProcessorManifest(options.getFacilitatorManifestURL());

      if (manifestFacilitator.isAwsBucket()) {
        options.setFacilitatorAwsBucketRegion(manifestFacilitator.getAwsBucketRegion());
        options.setFacilitatorAwsBucketName(manifestFacilitator.getAwsBucketName());
        options.setFacilitatorAwsBucketRole(manifestFacilitator.getAwsRole());
      }

      options.setFacilitatorOutput(
          getOutputPrefix(options.getFacilitatorOutput(), manifestFacilitator));
    }
  }

  private static PCollection<KV<DataShareMetadata, Iterable<DataShare>>> groupIntoBatches(
      PCollection<KV<DataShareMetadata, DataShare>> serializedDataShares, long batchSize) {
    return serializedDataShares
        .apply(
            "KeyOnMetadata",
            MapElements.via(
                new SimpleFunction<
                    KV<DataShareMetadata, DataShare>,
                    KV<String, KV<DataShareMetadata, DataShare>>>() {
                  @Override
                  public KV<String, KV<DataShareMetadata, DataShare>> apply(
                      KV<DataShareMetadata, DataShare> input) {
                    return KV.of(input.getKey().toString(), input);
                  }
                }))
        .apply("GroupIntoBatches", GroupIntoBatches.ofSize(batchSize))
        .apply(
            "FlattenAndNumberBatches",
            ParDo.of(
                new DoFn<
                    KV<String, Iterable<KV<DataShareMetadata, DataShare>>>,
                    KV<DataShareMetadata, Iterable<DataShare>>>() {

                  // A state cell holding latest used batch number
                  @StateId("batchNumber")
                  private final StateSpec<ValueState<Integer>> batchNumberSpec =
                      StateSpecs.value(VarIntCoder.of());

                  @ProcessElement
                  public void processElement(
                      ProcessContext c, @StateId("batchNumber") ValueState<Integer> batchNumber) {
                    List<DataShare> packets = new ArrayList<>();
                    DataShareMetadata metadata = null;
                    for (KV<DataShareMetadata, DataShare> entry : c.element().getValue()) {
                      if (metadata == null) {
                        metadata = entry.getKey();
                      }
                      packets.add(entry.getValue());
                    }
                    // create output metadata with incremented batch number
                    DataShareMetadata.Builder updatedMetadataBuilder = metadata.toBuilder();
                    Integer updatedBatchNum = batchNumber.read();
                    if (updatedBatchNum == null) {
                      // first access
                      updatedBatchNum = 1;
                    } else {
                      updatedBatchNum = updatedBatchNum + 1;
                    }
                    batchNumber.write(updatedBatchNum);
                    updatedMetadataBuilder.setBatchNumber(updatedBatchNum);
                    c.output(KV.of(updatedMetadataBuilder.build(), packets));
                  }
                }));
  }

  // Override manifest bucket (if present) with explicitly specified output path flag
  private static String getOutputPrefix(String outputValue, DataProcessorManifest manifest) {
    if (!"".equals(outputValue)) {
      return outputValue;
    }
    if (manifest == null) {
      throw new IllegalArgumentException("Must specify either output option or manifest url");
    }
    if (manifest.isAwsBucket()) {
      return "s3://" + manifest.getAwsBucketName();
    }
    return manifest.getIngestionBucket();
  }
}
