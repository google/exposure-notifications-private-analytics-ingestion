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

package com.google.exposurenotification.privateanalytics.ingestion.pipeline;

import static com.google.common.truth.Truth.assertThat;
import static com.google.exposurenotification.privateanalytics.ingestion.pipeline.FirestoreConnector.formatDateTime;
import static org.junit.Assert.assertThrows;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPagedResponse;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.firestore.v1.CreateDocumentRequest;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.GetDocumentRequest;
import com.google.firestore.v1.ListDocumentsRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link DeletionPipeline}. */
@RunWith(JUnit4.class)
public class DeletionPipelineIT {

  // Randomize document creation time to avoid collisions between simultaneously running tests.
  // FirestoreReader will query all documents with created times within one hour of this time.
  static final long CREATION_TIME = ThreadLocalRandom.current().nextLong(0L, 1500000000L);
  static final long DURATION = 10800L;
  static final String PROJECT = System.getenv("PROJECT");
  // Randomize test collection name to avoid collisions between simultaneously running tests.
  static final String TEST_COLLECTION_NAME =
      "uuid" + UUID.randomUUID().toString().replace("-", "_");
  static final String KEY_RESOURCE_NAME = System.getenv("KEY_RESOURCE_NAME");

  static List<Document> documentList;
  static FirestoreClient client;

  public transient IngestionPipelineOptions testOptions =
      TestPipeline.testingPipelineOptions().as(IngestionPipelineOptions.class);

  @Rule public final transient TestPipeline testPipeline = TestPipeline.fromOptions(testOptions);

  @Before
  public void setUp() throws IOException {
    documentList = new ArrayList<>();
    client = getFirestoreClient();
  }

  @After
  public void tearDown() {
    FirestoreClientTestUtils.shutdownFirestoreClient(client);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFirestoreDeleterDeletesDocs() throws InterruptedException {
    IngestionPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(IngestionPipelineOptions.class);
    options.setStartTime(CREATION_TIME);
    options.setProject(PROJECT);
    options.setDuration(DURATION);
    options.setKeyResourceName(KEY_RESOURCE_NAME);
    int numDocs = 50;
    seedDatabase(numDocs);

    PipelineResult result = DeletionPipeline.runDeletionPipeline(options);
    result.waitUntilFinish();

    // Assert that processed documents have been deleted.
    documentList.forEach(
        doc -> {
          String name = doc.getName();
          assertThrows(NotFoundException.class, () -> fetchDocumentFromFirestore(name, client));
        });
    long documentsDeleted =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(FirestoreConnector.class, "documentsDeleted"))
                    .build())
            .getCounters()
            .iterator()
            .next()
            .getCommitted();
    result
        .metrics()
        .queryMetrics(
            MetricsFilter.builder()
                .addNameFilter(MetricNameFilter.named(FirestoreConnector.class, "documentsDeleted"))
                .build())
        .getCounters()
        .iterator()
        .next()
        .getCommitted();
    assertThat(documentsDeleted).isEqualTo(numDocs);
    cleanUpParentResources(client);
  }

  private static FirestoreClient getFirestoreClient() throws IOException {
    FirestoreSettings settings =
        FirestoreSettings.newBuilder()
            .setCredentialsProvider(
                FixedCredentialsProvider.create(GoogleCredentials.getApplicationDefault()))
            .build();
    return FirestoreClient.create(settings);
  }

  private static void cleanUpParentResources(FirestoreClient client) {
    ListDocumentsPagedResponse documents =
        client.listDocuments(
            ListDocumentsRequest.newBuilder()
                .setParent("projects/" + PROJECT + "/databases/(default)/documents")
                .setCollectionId(TEST_COLLECTION_NAME)
                .build());
    documents.iterateAll().forEach(document -> client.deleteDocument(document.getName()));
  }

  private static Document fetchDocumentFromFirestore(String path, FirestoreClient client) {
    return client.getDocument(GetDocumentRequest.newBuilder().setName(path).build());
  }

  private static void seedDatabase(int numDocsToSeed) throws InterruptedException {
    // Adding a wait here to give the Firestore instance time to initialize before attempting
    // to connect.
    TimeUnit.SECONDS.sleep(1);

    for (int i = 1; i <= numDocsToSeed; i++) {
      Document doc = Document.getDefaultInstance();
      documentList.add(
          client.createDocument(
              CreateDocumentRequest.newBuilder()
                  .setCollectionId(formatDateTime(CREATION_TIME))
                  .setDocumentId("metric1")
                  .setDocument(doc)
                  .setParent(
                      "projects/"
                          + PROJECT
                          + "/databases/(default)/documents/"
                          + TEST_COLLECTION_NAME
                          + "/testDoc"
                          + i)
                  .build()));
    }
  }
}
