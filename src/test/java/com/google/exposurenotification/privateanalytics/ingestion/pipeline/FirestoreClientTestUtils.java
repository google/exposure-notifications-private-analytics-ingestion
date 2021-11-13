package com.google.exposurenotification.privateanalytics.ingestion.pipeline;

import com.google.cloud.firestore.v1.FirestoreClient;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FirestoreClientTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(FirestoreClientTestUtils.class);

  static final Duration FIRESTORE_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

  static void shutdownFirestoreClient(FirestoreClient client) {
    client.shutdown();
    LOG.info("Waiting for FirestoreClient to shutdown.");
    try {
      client.awaitTermination(FIRESTORE_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for client shutdown", e);
      Thread.currentThread().interrupt();
    }
  }
}
