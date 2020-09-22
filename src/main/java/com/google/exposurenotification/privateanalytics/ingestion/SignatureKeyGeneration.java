package com.google.exposurenotification.privateanalytics.ingestion;

import com.google.cloud.kms.v1.AsymmetricSignResponse;
import com.google.cloud.kms.v1.CryptoKeyVersionName;
import com.google.cloud.kms.v1.Digest;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Generate signature for input message.
 */
public class SignatureKeyGeneration extends DoFn<String, byte[]> {

  private KeyManagementServiceClient client;
  private CryptoKeyVersionName keyVersionName;

  @StartBundle
  public void startBundle(StartBundleContext context) throws IOException {
    client = KeyManagementServiceClient.create();
    IngestionPipelineOptions options = context.getPipelineOptions().as(IngestionPipelineOptions.class);
    keyVersionName = CryptoKeyVersionName.of(
        options.getProjectId().get(),
        options.getLocationId().get(),
        options.getKeyRingId().get(),
        options.getKeyId().get(),
        options.getKeyVersionId().get());
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws  NoSuchAlgorithmException {
    //TODO(amanraj): What happens if we fail an individual signing, should we fail that batch or the
    // full pipeline?
    MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
    byte[] hash = sha256.digest(c.element().getBytes(StandardCharsets.UTF_8));
    Digest digest = Digest.newBuilder().setSha256(ByteString.copyFrom(hash)).build();

    AsymmetricSignResponse result = client.asymmetricSign(keyVersionName, digest);
    c.output(result.getSignature().toByteArray());
  }
}
