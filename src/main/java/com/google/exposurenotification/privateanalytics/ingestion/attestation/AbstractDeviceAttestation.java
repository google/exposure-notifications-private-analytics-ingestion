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
