package com.google.exposurenotification.privateanalytics.ingestion;

import com.google.cloud.kms.v1.AsymmetricSignResponse;
import com.google.cloud.kms.v1.CryptoKeyVersionName;
import com.google.cloud.kms.v1.Digest;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.io.FileUtils;

/**
 * Generate signature for contents of input filenames and write them to files.
 */
public class SignatureKeyGeneration extends DoFn<String, Void> {

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
  public void processElement(ProcessContext c) throws NoSuchAlgorithmException, IOException {
    String filename = c.element();
    String content = FileUtils.readFileToString(new File(filename));

    //TODO(amanraj): What happens if we fail an individual signing, should we fail that batch or the
    // full pipeline?
    MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
    byte[] hash = sha256.digest(content.getBytes());
    Digest digest = Digest.newBuilder().setSha256(ByteString.copyFrom(hash)).build();

    AsymmetricSignResponse result = client.asymmetricSign(keyVersionName, digest);

    //TODO(amanraj) move away from depending on the filename.
    String targetFilename = filename.split("\\.")[0] + ".sig";
    FileUtils.writeByteArrayToFile(new File(targetFilename), result.getSignature().toByteArray());
  }
}
