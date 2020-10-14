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

import com.google.exposurenotification.privateanalytics.ingestion.DataShare.DataShareMetadata;
import com.google.exposurenotification.privateanalytics.ingestion.FirestoreConnector.FirestoreDeleter;
import com.google.exposurenotification.privateanalytics.ingestion.FirestoreConnector.FirestoreReader;
import com.google.exposurenotification.privateanalytics.ingestion.SerializationFunctions.SerializeDataShareFn;
import com.google.exposurenotification.privateanalytics.ingestion.SerializationFunctions.SerializePacketHeaderSignature;
import java.util.ArrayList;
import java.util.List;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.apache.beam.runners.core.construction.renderer.PipelineDotRenderer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

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

  static PipelineResult runIngestionPipeline(IngestionPipelineOptions options, List<String> metrics) {
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
    for (String metric: metrics) {
      PCollection<DataShare> metricDataShares = dataShares
          .apply("FilterDataSharesByMetric=" + metric, Filter.by(inputDataShare ->
              inputDataShare.getDataShareMetadata().getMetricName().equals(metric)));

      // TODO(amanraj): Make separate batch UUID for each batch of data shares.
      PCollection<KV<DataShareMetadata, List<PrioDataSharePacket>>> serializedDataShares =
              processDataShares(metricDataShares, options, metric)
                      .apply("SerializeDataSharesFor" + metric,
                              ParDo.of(new SerializeDataShareFn(metric)));
      //TODO try to move this to a flag/options. Right now adding this to IngestionPipelineOption
      //not working as GroupIntoBatches.ofSize needs a long and we are unable to access option
      // values here.
      long batchSize = 1000L;
      //TODO set up a proper coder to have a cleaner GroupBy
      PCollection<KV<DataShareMetadata, List<List<PrioDataSharePacket>>>>
          datashareGroupedByMetadata = groupIntoBatches(metric, serializedDataShares, batchSize);

      datashareGroupedByMetadata.apply("SerializePacketHeaderSigFor_metric=" + metric,
          ParDo.of(new SerializePacketHeaderSignature(
                  options.getPHAOutput(),
                  options.getFacilitatorOutput(),
                  options.getStartTime(),
                  options.getDuration())));
    }
    // TODO(larryjacobs): use org.apache.beam.sdk.transforms.Wait to only delete when pipeline successfully writes batch files
    dataShares.apply(new FirestoreDeleter());

    LOG.info("DOT graph representation:\n" + PipelineDotRenderer.toDotString(pipeline));
    return pipeline.run();
  }

  public static void main(String[] args) {
    IngestionPipelineFlags flags = new IngestionPipelineFlags();
    new CommandLine(flags).parseArgs(args);
    PipelineOptionsFactory.register(IngestionPipelineOptions.class);
    IngestionPipelineOptions options =
        PipelineOptionsFactory
                .fromArgs(flags.pipelineOptionsParams)
                .withValidation()
                .as(IngestionPipelineOptions.class);
    try {
      PipelineResult result = runIngestionPipeline(options, flags.metrics);
      result.waitUntilFinish();
      MetricResults metrics = result.metrics();
      String metricsInfo = "";
      for (MetricResult metricResult: metrics.allMetrics().getCounters()) {
        metricsInfo += metricResult.toString() + "\n";
      }
      LOG.info(metricsInfo);
    } catch (UnsupportedOperationException ignore) {
      // Known issue that this can throw when generating a template:
      // https://issues.apache.org/jira/browse/BEAM-9337
    } catch (Exception e) {
      LOG.error("Exception thrown during pipeline run.", e);
    }
  }

  private static PCollection<KV<DataShareMetadata, List<List<PrioDataSharePacket>>>> groupIntoBatches(
      String metric,
      PCollection<KV<DataShareMetadata, List<PrioDataSharePacket>>> serializedDataShares,
      long batchSize) {
    return serializedDataShares
        .apply("AddMetadataStringAsKeys_metric=" + metric, MapElements.via(
            new SimpleFunction<KV<DataShareMetadata, List<PrioDataSharePacket>>,
                KV<String, KV<DataShareMetadata, List<PrioDataSharePacket>>>>() {
              @Override
              public KV<String, KV<DataShareMetadata, List<PrioDataSharePacket>>> apply(
                  KV<DataShareMetadata, List<PrioDataSharePacket>> input) {
                return KV.of(input.getKey().toString(), input);
              }
            }))
        .apply("GroupIntoBatches_metric=" + metric, GroupIntoBatches.ofSize(batchSize))
        .apply("RemoveStringFromKey_metric=" + metric, MapElements.via(
            new SimpleFunction<KV<String, Iterable<KV<DataShareMetadata, List<PrioDataSharePacket>>>>,
                KV<DataShareMetadata, List<List<PrioDataSharePacket>>>>() {
              @Override
              public KV<DataShareMetadata, List<List<PrioDataSharePacket>>> apply(
                  KV<String, Iterable<KV<DataShareMetadata, List<PrioDataSharePacket>>>> input) {
                List<List<PrioDataSharePacket>> packets = new ArrayList<>();
                DataShareMetadata metadata = DataShareMetadata.builder().build();
                for(KV<DataShareMetadata, List<PrioDataSharePacket>> entry : input.getValue()) {
                  metadata = entry.getKey();
                  packets.add(entry.getValue());
                }
                return KV.of(metadata, packets);
              }
            }));
  }
}
