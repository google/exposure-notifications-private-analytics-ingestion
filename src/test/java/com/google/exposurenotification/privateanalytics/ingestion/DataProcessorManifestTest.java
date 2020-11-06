package com.google.exposurenotification.privateanalytics.ingestion;

import static com.google.common.truth.Truth.assertThat;

import java.net.URL;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DataProcessorManifest}. */
@RunWith(JUnit4.class)
public class DataProcessorManifestTest {

  @Test
  public void testParsing() {
    URL manifestUrl =
        getClass()
            .getResource(
                "/java/com/google/exposurenotification/privateanalytics/ingestion/test-manifest.json");
    DataProcessorManifest manifest = new DataProcessorManifest(manifestUrl.toString());
    assertThat(manifest.getIngestionBucket())
        .isEqualTo("s3://us-west-1/prio-demo-gcp-test-pha-1-ingestor-1-ingestion");
    assertThat(manifest.mapEncryptionKeyId("55NdHuhCjyR3PtTL0A7WRiaIgURhTmlkNw5dbFsKL70="))
        .isEqualTo("demo-gcp-test-pha-1-ingestion-packet-decryption-key");
  }
}
