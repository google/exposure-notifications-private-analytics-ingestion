/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.exposurenotification.privateanalytics.ingestion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.WriteResult;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline.CountWords;
import com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline.ExtractWordsFn;
import com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline.FormatAsTextFn;
import com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline.IngestionPipelineOptions;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link IngestionPipeline}.
 */
@RunWith(JUnit4.class)
public class IngestionPipelineTest {
  public static final String FIREBASE_PROJECT_ID = "appa-firebase-test";
  public static final String SERVICE_ACCOUNT_KEY_PATH = "PATH/TO/SERVICE_ACCOUNT_KEY.json";
  public static final String TEST_COLLECTION_NAME = "test-uuid";
  public static final String DOC_1_NAME = "test";
  public static final String DOC_2_NAME = "metric1";

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testExtractWordsFn() throws Exception {
    List<String> words = Arrays.asList(" some  input  words ", " ", " cool ", " foo", " bar");
    PCollection<String> output =
        p.apply(Create.of(words).withCoder(StringUtf8Coder.of()))
            .apply(ParDo.of(new ExtractWordsFn()));
    PAssert.that(output).containsInAnyOrder("some", "input", "words", "cool", "foo", "bar");
    p.run().waitUntilFinish();
  }

  static final String[] WORDS_ARRAY =
      new String[]{
          "hi there", "hi", "hi sue bob",
          "hi sue", "", "bob hi"
      };

  static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

  static final String[] COUNTS_ARRAY = new String[]{"hi: 5", "there: 1", "sue: 2", "bob: 2"};

  @Rule
  public TestPipeline p = TestPipeline.create();

  private static IngestionPipelineOptions options;
  private static Firestore db;

  @BeforeClass
  public static void setUp() throws Exception {
    IngestionPipelineOptions options = TestPipeline.testingPipelineOptions().as(
        IngestionPipelineOptions.class);
    options.setFirebaseProjectId(StaticValueProvider.of(FIREBASE_PROJECT_ID));
    options.setServiceAccountKey(StaticValueProvider.of(SERVICE_ACCOUNT_KEY_PATH));
    db = IngestionPipeline.initializeFirestore(options);
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testCountWords() throws Exception {
    PCollection<String> input = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()));

    PCollection<String> output =
        input.apply(new CountWords()).apply(MapElements.via(new FormatAsTextFn()));

    PAssert.that(output).containsInAnyOrder(COUNTS_ARRAY);
    p.run().waitUntilFinish();
  }

  @Test
  public void testIngestionPipeline() throws Exception {
    File outputFile = tmpFolder.newFile();
    IngestionPipelineOptions options = TestPipeline.testingPipelineOptions().as(
        IngestionPipelineOptions.class);
    options.setOutput(StaticValueProvider.of(getFilePath(outputFile.getAbsolutePath())));
    options.setFirebaseProjectId(StaticValueProvider.of(FIREBASE_PROJECT_ID));
    options.setServiceAccountKey(StaticValueProvider.of(SERVICE_ACCOUNT_KEY_PATH));

    IngestionPipeline.runIngestionPipeline(options);
  }

  @Test
  public void testReadDocumentsFromFirestore() throws Exception {
    IngestionPipelineOptions options = TestPipeline.testingPipelineOptions().as(
        IngestionPipelineOptions.class);
    options.setFirebaseProjectId(StaticValueProvider.of(FIREBASE_PROJECT_ID));
    options.setServiceAccountKey(StaticValueProvider.of(SERVICE_ACCOUNT_KEY_PATH));
    CollectionReference uuids = seedDatabase(db);

    List<String> docs = IngestionPipeline.readDocumentsFromFirestore(db, TEST_COLLECTION_NAME);

    assertEquals(docs.size(), 2);
    assertTrue(docs.contains(DOC_1_NAME));
    assertTrue(docs.contains(DOC_2_NAME));
    // tear-down
    cleanupCollection(uuids, 2);
  }

  private String getFilePath(String filePath) {
    if (filePath.contains(":")) {
      return filePath.replace("\\", "/").split(":", -1)[1];
    }
    return filePath;
  }

  /**
   * Creates test-users collection and adds sample documents to test queries.
   */
  private CollectionReference seedDatabase(Firestore db) throws Exception {
    CollectionReference uuids = db.collection(TEST_COLLECTION_NAME);
    List<ApiFuture<WriteResult>> futures = new ArrayList<>();
    Map<String, Object> doc1Data = new HashMap<>();
    Map<String, Object> doc2Data = new HashMap<>();
    doc1Data.put("data", "data");
    doc2Data.put("data", "data");
    futures.add(
        uuids
            .document("test")
            .set(doc1Data));
    futures.add(
        uuids
            .document("metric1")
            .set(doc2Data));

    // block on documents successfully added so test isn't flaky.
    ApiFutures.allAsList(futures).get();

    return uuids;
  }

  /**
   * Deletes the given collection.
   * Batch size may be tuned based on document size (atmost 1MB) and application requirements.
   */
  void cleanupCollection(CollectionReference collection, int batchSize) {
    try {
      // retrieve a small batch of documents to avoid out-of-memory errors
      ApiFuture<QuerySnapshot> future = collection.limit(batchSize).get();
      int deleted = 0;
      // future.get() blocks on document retrieval
      List<QueryDocumentSnapshot> documents = future.get().getDocuments();
      for (QueryDocumentSnapshot document : documents) {
        document.getReference().delete();
        ++deleted;
      }
      if (deleted >= batchSize) {
        // retrieve and delete another batch
        cleanupCollection(collection, batchSize);
      }
    } catch (Exception e) {
      System.err.println("Error deleting collection : " + e.getMessage());
    }
  }
}
