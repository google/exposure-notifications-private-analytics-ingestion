package com.google.exposurenotification.privateanalytics.ingestion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteResult;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline.IngestionPipelineOptions;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import com.google.firebase.database.FirebaseDatabase;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link IngestionPipeline}.
 */
@RunWith(JUnit4.class)
public class IngestionPipelineIT {

  static final String FIREBASE_PROJECT_ID = "emulator-test-project";
  static final String SERVICE_ACCOUNT_KEY_PATH = "PATH/TO/SERVICE_ACCOUNT_KEY.json";
  static final String TEST_COLLECTION_NAME = "test-uuid";
  static final String DOC_1_NAME = "test";
  static final String DOC_2_NAME = "metric1";

  static Firestore db;

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUp() throws Exception {
    FirebaseOptions options = FirebaseOptions.builder()
        .setProjectId(FIREBASE_PROJECT_ID)
        .setCredentials(GoogleCredentials.newBuilder().build())
        .build();
    FirebaseApp.initializeApp(options);
    db = FirestoreClient.getFirestore();
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
   * Deletes the given collection. Batch size may be tuned based on document size (atmost 1MB) and
   * application requirements.
   */
  private void cleanupCollection(CollectionReference collection, int batchSize) {
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
