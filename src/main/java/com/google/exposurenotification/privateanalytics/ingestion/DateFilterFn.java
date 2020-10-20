package com.google.exposurenotification.privateanalytics.ingestion;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A DoFn that filters documents in particular time window */
public class DateFilterFn extends DoFn<DataShare, DataShare> {

  private static final Logger LOG = LoggerFactory.getLogger(DateFilterFn.class);

  private final Map<String, Counter> dateFilterIncluded = new HashMap<>();
  private final Map<String, Counter> dateFilterExcluded = new HashMap<>();

  @ProcessElement
  public void processElement(ProcessContext c) {
    String metricName = c.element().getDataShareMetadata().getMetricName();
    if(!dateFilterIncluded.containsKey(metricName)) {
      dateFilterIncluded.put(metricName,
          Metrics.counter(DateFilterFn.class, "dateFilterIncluded_" + metricName));
      dateFilterExcluded.put(metricName,
          Metrics.counter(DateFilterFn.class, "dateFilterExcluded_" + metricName));
    }

    if (c.element().getCreated() == null || c.element().getCreated() == 0) {
      return;
    }
    IngestionPipelineOptions options = c.getPipelineOptions().as(IngestionPipelineOptions.class);

    long startTime = options.getStartTime().get();
    long duration = options.getDuration().get();

    if (c.element().getCreated() >= startTime && c.element().getCreated() < startTime + duration) {
      LOG.debug("Included: " + c.element());
      dateFilterIncluded.get(metricName).inc();
      c.output(c.element());
    } else {
      LOG.trace("Excluded: " + c.element());
      dateFilterExcluded.get(metricName).inc();
    }
  }
}
