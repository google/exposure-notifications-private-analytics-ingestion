package com.google.exposurenotification.privateanalytics.ingestion.attestation;

import com.google.exposurenotification.privateanalytics.ingestion.DataShare;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
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
      Metrics.counter(KeyAttestation.class, "processed");
  protected static final Counter acceptedCounter =
      Metrics.counter(KeyAttestation.class, "accepted");
}
