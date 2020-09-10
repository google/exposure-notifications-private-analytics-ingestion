package com.google.exposurenotification.privateanalytics.ingestion;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteResult;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
    seedDatabase(db);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIngestionPipeline() throws Exception {
    File outputFile = tmpFolder.newFile();
    IngestionPipelineOptions options = TestPipeline.testingPipelineOptions().as(
        IngestionPipelineOptions.class);
    options.setOutput(StaticValueProvider.of(getFilePath(outputFile.getAbsolutePath())));
    options.setFirebaseProjectId(StaticValueProvider.of(FIREBASE_PROJECT_ID));
    options.setServiceAccountKey(StaticValueProvider.of(SERVICE_ACCOUNT_KEY_PATH));

    IngestionPipeline.runIngestionPipeline(options);
    // TODO: assert some fun stuff about what's in tmpFolder
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
  private static CollectionReference seedDatabase(Firestore db) throws Exception {
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
}
