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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
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
  static final long DURATION = 10000L;
  static final String FIREBASE_PROJECT_ID = "emulator-test-project";
  static final long MINIMUM_PARTICIPANT_COUNT = 1L;
  static final String SERVICE_ACCOUNT_KEY_PATH = "PATH/TO/SERVICE_ACCOUNT_KEY.json";
  static final String TEST_COLLECTION_NAME = "test-uuid";

  static Firestore db;

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUp() {
    FirebaseOptions options = FirebaseOptions.builder()
        .setProjectId(FIREBASE_PROJECT_ID)
        .setCredentials(GoogleCredentials.newBuilder().build())
        .build();
    FirebaseApp.initializeApp(options);
    db = FirestoreClient.getFirestore();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIngestionPipeline() throws IOException, ExecutionException, InterruptedException {
    Map<String, PrioDataSharePacket> inputDataSharePackets = seedDatabaseAndReturnEntryVal(db);

    File outputFile = tmpFolder.newFile();
    IngestionPipelineOptions options = TestPipeline.testingPipelineOptions().as(
        IngestionPipelineOptions.class);
    options.setOutput(StaticValueProvider.of(getFilePath(outputFile.getAbsolutePath())));
    options.setFirebaseProjectId(StaticValueProvider.of(FIREBASE_PROJECT_ID));
    options.setServiceAccountKey(StaticValueProvider.of(SERVICE_ACCOUNT_KEY_PATH));
    options.setMetric(StaticValueProvider.of(TEST_COLLECTION_NAME));
    options.setMinimumParticipantCount(StaticValueProvider.of(MINIMUM_PARTICIPANT_COUNT));
    options.setStartTime(StaticValueProvider.of(CREATION_TIME));
    options.setDuration(StaticValueProvider.of(DURATION));

    IngestionPipeline.runIngestionPipeline(options);

    Map<String, PrioDataSharePacket> actualDataSharepackets = readOutput();
    for(Map.Entry<String, PrioDataSharePacket> entry : actualDataSharepackets.entrySet()) {
      Assert.assertTrue("Output contains data which is not present in input", inputDataSharePackets.containsKey(entry.getKey()));
      comparePrioDataSharePacket(entry.getValue(), inputDataSharePackets.get(entry.getKey()));
    }
  }

  private Map<String, PrioDataSharePacket> readOutput() throws IOException {
    Map<String, PrioDataSharePacket> result = new HashMap<>();
    Stream<Path> paths = Files.walk(Paths.get(tmpFolder.getRoot().getPath()));
    List<Path> pathList = paths.filter(Files::isRegularFile).collect(Collectors.toList());
    for(Path path : pathList) {
      if(path.toString().endsWith(".avro")) {
        List<PrioDataSharePacket> packets = PrioSerializer.deserializeDataSharePackets(path.toString());
        result.putAll(packets.stream().collect(Collectors.toMap(packet -> packet.getUuid().toString(), Function.identity())));
      }
    }
    return result;
  }

  private String getFilePath(String filePath) {
    if (filePath.contains(":")) {
      return filePath.replace("\\", "/").split(":", -1)[1];
    }
    return filePath;
  }

  /**
   * Creates test-users collection and adds sample documents to test queries. Returns entry value in form of {@link Map<String, PrioDataSharePacket>}.
   */
  private static Map<String, PrioDataSharePacket> seedDatabaseAndReturnEntryVal(Firestore db)
      throws ExecutionException, InterruptedException {
    // Adding a wait here to give the Firestore instance time to initialize before attempting
    // to connect.
    TimeUnit.SECONDS.sleep(1);

    WriteBatch batch = db.batch();

    Map<String, Object> docData = new HashMap<>();
    List<DocumentReference> listDocReference = new ArrayList<>();
    for(int i = 1; i <= 2; i++) {
      docData.put("id", "id" + i);
      docData.put("payload", getSamplePayload("uuid" + i, CREATION_TIME));
      DocumentReference reference = db.collection(TEST_COLLECTION_NAME).document("doc" + i);
      batch.set(reference, docData);
      listDocReference.add(reference);
    }

    ApiFuture<List<WriteResult>> future = batch.commit();
    // future.get() blocks on batch commit operation
    future.get();

    Map<String, PrioDataSharePacket> dataShareByUuid = new HashMap<>();
    for(DocumentReference reference : listDocReference) {
      DataShare dataShare = DataShare.from(reference.get().get());
      PrioDataSharePacket packet = PrioDataSharePacket.newBuilder()
          .setEncryptionKeyId("hardCodedID")
          .setRPit(dataShare.getRPit())
          .setUuid(dataShare.getUuid())
          .setEncryptedPayload(ByteBuffer.wrap(new byte[] {0x01, 0x02, 0x03, 0x04, 0x05}))
          .build();
      dataShareByUuid.put(dataShare.getUuid(), packet);
    }

    return dataShareByUuid;
  }

  private static void comparePrioDataSharePacket(PrioDataSharePacket first, PrioDataSharePacket second) {
    Assert.assertEquals(first.getUuid().toString(),second.getUuid().toString());
    Assert.assertEquals(first.getEncryptedPayload(), second.getEncryptedPayload());
    Assert.assertEquals(first.getEncryptionKeyId().toString(), second.getEncryptionKeyId().toString());
  }

  private static Map<String, Object> getSamplePayload(String uuid, long timestampSeconds) {
    Map<String, Object> samplePayload = new HashMap<>();

    Map<String, Object> samplePrioParams = new HashMap<>();
    samplePrioParams.put(DataShare.PRIME, 4293918721L);
    samplePrioParams.put(DataShare.BINS, 2L);
    samplePrioParams.put(DataShare.EPSILON, 5.2933D);
    samplePrioParams.put(DataShare.NUMBER_OF_SERVERS, 2L);
    samplePrioParams.put(DataShare.HAMMING_WEIGHT, 1L);
    samplePayload.put(DataShare.PRIO_PARAMS, samplePrioParams);


    List<Map<String, String>> sampleEncryptedDataShares = new ArrayList<>();

    Map<String, String> sampleDataShare1 = new HashMap<>();
    sampleDataShare1.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId1");
    sampleDataShare1.put(DataShare.PAYLOAD, "fakePayload1");
    sampleEncryptedDataShares.add(sampleDataShare1);

    Map<String, String> sampleDataShare2 = new HashMap<>();
    sampleDataShare2.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId2");
    sampleDataShare2.put(DataShare.PAYLOAD, "fakePayload2");
    sampleEncryptedDataShares.add(sampleDataShare2);

    samplePayload.put(DataShare.CREATED, Timestamp.ofTimeSecondsAndNanos(timestampSeconds, 0));
    samplePayload.put(DataShare.UUID, uuid);
    samplePayload.put(DataShare.ENCRYPTED_DATA_SHARES, sampleEncryptedDataShares);

    return samplePayload;
  }
}
