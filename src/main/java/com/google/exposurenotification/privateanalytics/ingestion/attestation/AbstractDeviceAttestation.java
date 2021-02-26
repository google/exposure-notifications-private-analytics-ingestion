package com.google.exposurenotification.privateanalytics.ingestion.attestation;

import com.google.exposurenotification.privateanalytics.ingestion.model.DataShare;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * Class to extend to implement some form of check that data originated from a real device.
 *
 * <p>We use a PTransform rather than a Filter to allow flexibility in implementations. E.g., an
 * implementation might want to compute various distributions over the set of data coming in rather
 * than make a strictly local decision as to whether a given DataShare is attested.
 */
public abstract class AbstractDeviceAttestation
    extends PTransform<PCollection<DataShare>, PCollection<DataShare>> {

  // Counters for the number of elements processed and eventually accepted.
  protected static final Counter processedCounter =
      Metrics.counter(AbstractDeviceAttestation.class, "processed");
  protected static final Counter acceptedCounter =
      Metrics.counter(AbstractDeviceAttestation.class, "accepted");

  /** @return a non-null class object if the attestation has pipeline options to be registered */
  public abstract Class<? extends PipelineOptions> getOptionsClass();
}
