package com.google.exposurenotification.privateanalytics.ingestion.attestation.key;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface KeyAttestationPipelineOptions extends PipelineOptions {

  /** Package name to be verified in the key attestation certificate. */
  @Description("Package name of the Android application.")
  @Default.String("")
  String getPackageName();

  void setPackageName(String value);

  /**
   * Package digest (HEX(SHA256)) of the Android application, to be verified in the key attestation
   * certificate.
   */
  @Description("Package signature digest of the Android application.")
  @Default.String("")
  String getPackageSignatureDigest();

  void setPackageSignatureDigest(String value);
}
