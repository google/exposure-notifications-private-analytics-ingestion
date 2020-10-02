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
