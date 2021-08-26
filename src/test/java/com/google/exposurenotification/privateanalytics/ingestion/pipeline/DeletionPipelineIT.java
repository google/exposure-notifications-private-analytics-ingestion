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
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.DatabaseRootName;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.GetDocumentRequest;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.Write;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
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
  static final DatabaseRootName DATABASE_ROOT_NAME = DatabaseRootName.of(PROJECT, "(default)");
  static final String BASE_COLLECTION_NAME =
      String.format(
          "%s/documents/%s/testDoc/%s",
          DATABASE_ROOT_NAME, TEST_COLLECTION_NAME, formatDateTime(CREATION_TIME));

  List<String> documentNames;
  FirestoreClient client;

  public transient IngestionPipelineOptions testOptions =
      TestPipeline.testingPipelineOptions().as(IngestionPipelineOptions.class);

  @Rule public final transient TestPipeline testPipeline = TestPipeline.fromOptions(testOptions);

  @Before
  public void setUp() throws IOException {
    documentNames = new ArrayList<>();
    client = getFirestoreClient();
  }

  @After
  public void tearDown() {
    FirestoreClientTestUtils.shutdownFirestoreClient(client);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFirestoreDeleterDeletesDocs() throws InterruptedException {
    testOptions.as(DataflowPipelineOptions.class).setMaxNumWorkers(1);
    testOptions.setStartTime(CREATION_TIME);
    testOptions.setProject(PROJECT);
    testOptions.setDuration(DURATION);
    testOptions.setKeyResourceName(KEY_RESOURCE_NAME);
    int numDocs = 500;
    seedDatabase(numDocs);

    DeletionPipeline.buildDeletionPipeline(testOptions, testPipeline);
    PipelineResult result = testPipeline.run();
    result.waitUntilFinish();

    // Assert that processed documents have been deleted.
    documentNames.forEach(
        name ->
            assertThrows(NotFoundException.class, () -> fetchDocumentFromFirestore(name, client)));
    MetricNameFilter documentsDeletedMetricName =
        MetricNameFilter.named(
            "org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.BatchWrite", "writes_successful");
    long documentsDeleted =
        result
            .metrics()
            .queryMetrics(MetricsFilter.builder().addNameFilter(documentsDeletedMetricName).build())
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

  private void seedDatabase(int numDocsToSeed) throws InterruptedException {
    // Adding a wait here to give the Firestore instance time to initialize before attempting
    // to connect.
    TimeUnit.SECONDS.sleep(1);

    documentNames =
        IntStream.rangeClosed(1, numDocsToSeed)
            .mapToObj(i -> String.format("%s/metric%05d", BASE_COLLECTION_NAME, i))
            .collect(Collectors.toList());

    List<BatchWriteRequest> batchWriteRequests =
        Streams.stream(Iterables.partition(documentNames, 500))
            .map(
                names ->
                    names.stream()
                        .map(
                            name ->
                                Write.newBuilder()
                                    .setUpdate(Document.newBuilder().setName(name).build())
                                    .build())
                        .collect(Collectors.toList()))
            .map(
                writes ->
                    BatchWriteRequest.newBuilder()
                        .setDatabase(DATABASE_ROOT_NAME.toString())
                        .addAllWrites(writes)
                        .build())
            .collect(Collectors.toList());

    for (BatchWriteRequest batchWriteRequest : batchWriteRequests) {
      client.batchWrite(batchWriteRequest);
    }
  }
}
