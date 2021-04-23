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

import com.google.exposurenotification.privateanalytics.ingestion.attestation.AbstractDeviceAttestation;
import com.google.exposurenotification.privateanalytics.ingestion.model.DataShare;
import com.google.exposurenotification.privateanalytics.ingestion.model.DataShare.ConstructDataSharesFn;
import com.google.exposurenotification.privateanalytics.ingestion.model.DataShare.DataShareMetadata;
import com.google.exposurenotification.privateanalytics.ingestion.pipeline.FirestoreConnector.FirestorePartitionQueryCreation;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.RunQueryResponse;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
 * Facilitators.
 */
public class IngestionPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(IngestionPipeline.class);

  /**
   * Process input {@link PCollection<DataShare>}, and make them available for final serialization.
   * This encapsulates all the pipeline logic apart from I/O, for testability.
   */
  static PCollection<KV<DataShareMetadata, Iterable<DataShare>>> processDataShares(
      PCollection<DataShare> inputDataShares) {
    IngestionPipelineOptions options =
        (IngestionPipelineOptions) inputDataShares.getPipeline().getOptions();
    PCollection<DataShare> filteredShares =
        inputDataShares.apply("FilterDates", ParDo.of(new DateFilterFn()));
    if (options.getDeviceAttestation()) {
      ServiceLoader<AbstractDeviceAttestation> serviceLoader =
          ServiceLoader.load(AbstractDeviceAttestation.class);
      // In future we could chain together all attestation implementations found
      Optional<AbstractDeviceAttestation> attestationOption = serviceLoader.findFirst();
      if (attestationOption.isPresent()) {
        filteredShares = filteredShares.apply("DeviceAttestation", attestationOption.get());
      } else {
        LOG.warn("Requested attestation at commandline but no implementations found");
      }
    }
    PCollection<KV<DataShareMetadata, DataShare>> unbatchedShares =
        filteredShares.apply(
            "MapMetadata-",
            MapElements.via(
                new SimpleFunction<DataShare, KV<DataShareMetadata, DataShare>>() {
                  @Override
                  public KV<DataShareMetadata, DataShare> apply(DataShare input) {
                    return KV.of(input.getDataShareMetadata(), input);
                  }
                }));
    return groupIntoBatches(unbatchedShares, options.getBatchSize());
  }

  /** Perform the input, processing and output for the full ingestion pipeline. */
  static PipelineResult runIngestionPipeline(IngestionPipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    long startTime =
        IngestionPipelineOptions.calculatePipelineStart(
            options.getStartTime(), options.getDuration(), 1, Clock.systemUTC());
    PCollection<DataShare> dataShares =
        pipeline
            .apply(new FirestorePartitionQueryCreation(startTime))
            .apply(FirestoreIO.v1().read().partitionQuery().build())
            .apply(FirestoreIO.v1().read().runQuery().build())
            .apply(
                MapElements.via(
                    new SimpleFunction<RunQueryResponse, Document>() {
                      @Override
                      public Document apply(RunQueryResponse input) {
                        return input.hasDocument() ? input.getDocument() : null;
                      }
                    }))
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
    ServiceLoader<AbstractDeviceAttestation> serviceLoader =
        ServiceLoader.load(AbstractDeviceAttestation.class);
    Optional<AbstractDeviceAttestation> attestationOption = serviceLoader.findFirst();
    serviceLoader.forEach(x -> PipelineOptionsFactory.register(x.getOptionsClass()));
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
            // Using AutoValue leads to problems with the coder being non-deterministic (using
            // @DefaultSchema(AutoValueSchema.class), so just manually constructing the key
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
            "FlattenAndIdBatches",
            ParDo.of(
                new DoFn<
                    KV<String, Iterable<KV<DataShareMetadata, DataShare>>>,
                    KV<DataShareMetadata, Iterable<DataShare>>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    List<DataShare> packets = new ArrayList<>();
                    DataShareMetadata metadata = null;
                    for (KV<DataShareMetadata, DataShare> entry : c.element().getValue()) {
                      if (metadata == null) {
                        metadata = entry.getKey();
                      }
                      packets.add(entry.getValue());
                    }
                    /*
                     * It's useful to assign batch ids at this stage rather than in BatchWriterFn
                     * because if DataFlowRunner retries a batch, we'll write to the same
                     * destination. Of course with a random batch id (as opposed to, e.g., numbered
                     * batches) the destinations won't be the same if the entire pipeline is rerun.
                     */
                    DataShareMetadata updatedMetadata =
                        metadata.toBuilder().setBatchId(UUID.randomUUID().toString()).build();
                    c.output(KV.of(updatedMetadata, packets));
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
