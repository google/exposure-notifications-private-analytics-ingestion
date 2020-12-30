// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.exposurenotification.privateanalytics.ingestion;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A DoFn that filters data shares in a particular time window */
public class DateFilterFn extends DoFn<DataShare, DataShare> {

  private static final Logger LOG = LoggerFactory.getLogger(DateFilterFn.class);

  private final Map<String, Counter> dateFilterIncluded = new HashMap<>();
  private final Map<String, Counter> dateFilterExcluded = new HashMap<>();

  @ProcessElement
  public void processElement(ProcessContext c) {
    String metricName = c.element().getDataShareMetadata().getMetricName();
    if (!dateFilterIncluded.containsKey(metricName)) {
      dateFilterIncluded.put(
          metricName, Metrics.counter(DateFilterFn.class, "dateFilterIncluded_" + metricName));
      dateFilterExcluded.put(
          metricName, Metrics.counter(DateFilterFn.class, "dateFilterExcluded_" + metricName));
    }

    if (c.element().getCreatedMs() == null || c.element().getCreatedMs() == 0) {
      LOG.warn("Skipping document with no creation timestamp: {}", c.element().getPath());
      return;
    }
    IngestionPipelineOptions options = c.getPipelineOptions().as(IngestionPipelineOptions.class);

    long startTime =
        IngestionPipelineOptions.calculatePipelineStart(
            options.getStartTime(), options.getDuration(), 1, Clock.systemUTC());
    long duration = options.getDuration();

    if (c.element().getCreatedMs() >= startTime * 1000
        && c.element().getCreatedMs() < (startTime + duration) * 1000) {
      LOG.debug("Included: {}", c.element());
      dateFilterIncluded.get(metricName).inc();
      c.output(c.element());
    } else {
      LOG.trace("Excluded: {}", c.element());
      dateFilterExcluded.get(metricName).inc();
    }
  }
}
