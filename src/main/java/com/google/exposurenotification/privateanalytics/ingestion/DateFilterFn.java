package com.google.exposurenotification.privateanalytics.ingestion;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DoFn that filters documents in particular time window
 */
public class DateFilterFn extends DoFn<DataShare, DataShare> {

  private static final Logger LOG = LoggerFactory.getLogger(DateFilterFn.class);

  private final Counter dateFilterIncluded;
  private final Counter dateFilterExcluded;

  public DateFilterFn(String metric) {
    this.dateFilterIncluded = Metrics
            .counter(DateFilterFn.class, "dateFilterIncluded_" + metric);
    this.dateFilterExcluded = Metrics
            .counter(DateFilterFn.class, "dateFilterExcluded_" + metric);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    if (c.element().getCreated() == null || c.element().getCreated() == 0) {
      return;
    }
    IngestionPipelineOptions options = c.getPipelineOptions()
            .as(IngestionPipelineOptions.class);

    long startTime = options.getStartTime().get();
    long duration = options.getDuration().get();

    if (c.element().getCreated() >= startTime &&
        c.element().getCreated() < startTime + duration) {
      LOG.debug("Included: " + c.element());
      dateFilterIncluded.inc();
      c.output(c.element());
    } else {
      LOG.trace("Excluded: " + c.element());
      dateFilterExcluded.inc();
    }
  }
}
