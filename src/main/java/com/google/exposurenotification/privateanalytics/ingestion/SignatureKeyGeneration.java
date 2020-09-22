package com.google.exposurenotification.privateanalytics.ingestion;

import java.nio.ByteBuffer;
import org.abetterinternet.prio.v1.PrioIngestionSignature;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Generate {@link PrioIngestionSignature} for output files.
 */
public class SignatureKeyGeneration extends DoFn<String, PrioIngestionSignature> {
  @ProcessElement
  public PrioIngestionSignature processElement(ProcessContext c) {
    return PrioIngestionSignature.newBuilder()
        .setBatchHeaderSignature(ByteBuffer.wrap("sampleBatchHeaderSign".getBytes()))
        .setSignatureOfPackets(ByteBuffer.wrap("samplePacketsSign".getBytes()))
        .build();
  }
}
