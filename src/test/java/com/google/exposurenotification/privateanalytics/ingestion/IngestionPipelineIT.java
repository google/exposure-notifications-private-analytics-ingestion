/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.exposurenotification.privateanalytics.ingestion;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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

  static final long CREATION_TIME = 12345L;
  static final Timestamp CREATED = Timestamp.ofTimeSecondsAndNanos(CREATION_TIME, 0);
  static final String DOC_1_ID = "doc1";
  static final String DOC_2_ID = "doc2";
  static final String FIREBASE_PROJECT_ID = "emulator-test-project";
  static final long MINIMUM_PARTICIPANT_COUNT = 0L;
  static final long DURATION = 100000000L;
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
    options.setMetric(StaticValueProvider.of(TEST_COLLECTION_NAME));
    options.setMinimumParticipantCount(StaticValueProvider.of(MINIMUM_PARTICIPANT_COUNT));
    options.setDuration(StaticValueProvider.of(DURATION));

    IngestionPipeline.runIngestionPipeline(options);

    //TODO(larryjacobs): assert actualOutput == expected. Unable to make this assertion now
    // because the temporary outputFile is being deleted too soon after the pipeline finishes running.
    String actualOutput = readFile(outputFile);
    DataShare ds1 = DataShare.builder().setId(DOC_1_ID).setCreated(CREATION_TIME).build();
    DataShare ds2 = DataShare.builder().setId(DOC_2_ID).setCreated(CREATION_TIME).build();
  }

  private String readFile(File outputFile) throws IOException {
    FileReader fileReader = new FileReader(outputFile);
    char[] chars = new char[(int) outputFile.length()];
    fileReader.read(chars);
    return new String(chars);
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
  private static void seedDatabase(Firestore db) throws Exception {
    // Adding a wait here to give the Firestore instance time to initialize before attempting
    // to connect.
    TimeUnit.SECONDS.sleep(20);

    WriteBatch batch = db.batch();

    DocumentReference doc1 = db.collection(TEST_COLLECTION_NAME).document(DOC_1_ID);
    Map<String, Object> docData = new HashMap<>();
    docData.put("created", CREATED);
    batch.set(doc1, docData);

    DocumentReference doc2 = db.collection(TEST_COLLECTION_NAME).document(DOC_2_ID);
    batch.set(doc2, docData);
    docData.put("created", CREATED);

    ApiFuture<List<WriteResult>> future = batch.commit();
    // future.get() blocks on batch commit operation
    future.get();
  }
}
