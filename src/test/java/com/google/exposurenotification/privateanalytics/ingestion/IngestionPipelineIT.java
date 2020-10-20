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

import static com.google.exposurenotification.privateanalytics.ingestion.FirestoreConnector.formatDateTime;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.EncryptedShare;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.GetDocumentRequest;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/** Integration tests for {@link IngestionPipeline}. */
@RunWith(JUnit4.class)
public class IngestionPipelineIT {

  private static final Logger LOG = LoggerFactory.getLogger(IngestionPipeline.class);
  static final long CREATION_TIME = 12345L;
  static final long DURATION = 10000L;
  static final String FIREBASE_PROJECT_ID = System.getenv("FIREBASE_PROJECT_ID");
  static final int MINIMUM_PARTICIPANT_COUNT = 1;
  // Randomize test collection name to avoid collisions between simultaneously running tests.
  static final String TEST_COLLECTION_NAME =
      "uuid" + UUID.randomUUID().toString().replace("-", "_");
  // TODO(amanraj): figure out way to not check this in
  static final String KEY_RESOURCE_NAME =
      "projects/appa-ingestion/locations/global/keyRings/appa-signature-key-ring/cryptoKeys/appa-signature-key/cryptoKeyVersions/1";

  static Firestore db;
  static List<DocumentReference> listDocReference;

  @Rule public TemporaryFolder tmpFolderPha = new TemporaryFolder();
  @Rule public TemporaryFolder tmpFolderFac = new TemporaryFolder();

  private static IngestionPipelineFlags flags = new IngestionPipelineFlags();

  @BeforeClass
  public static void setUp() throws IOException {
    FirestoreOptions firestoreOptions =
        FirestoreOptions.getDefaultInstance().toBuilder()
            .setProjectId(FIREBASE_PROJECT_ID)
            .setCredentials(GoogleCredentials.getApplicationDefault())
            .build();
    db = firestoreOptions.getService();
    listDocReference = new ArrayList<>();
    new CommandLine(flags).parseArgs(new String[]{"batchSize=1000"});
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIngestionPipeline() throws Exception {
    File phaFile = tmpFolderPha.newFile();
    File facilitatorFile = tmpFolderFac.newFile();
    IngestionPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(IngestionPipelineOptions.class);
    List<String> forkedSharesFilePrefixes =
        Arrays.asList(
            getFilePath(phaFile.getAbsolutePath()), getFilePath(facilitatorFile.getAbsolutePath()));
    options.setPHAOutput(StaticValueProvider.of(phaFile.getAbsolutePath()));
    options.setFacilitatorOutput(StaticValueProvider.of(facilitatorFile.getAbsolutePath()));
    options.setFirebaseProjectId(StaticValueProvider.of(FIREBASE_PROJECT_ID));
    options.setMinimumParticipantCount(StaticValueProvider.of(MINIMUM_PARTICIPANT_COUNT));
    options.setStartTime(StaticValueProvider.of(CREATION_TIME));
    options.setDuration(StaticValueProvider.of(DURATION));
    options.setKeyResourceName(StaticValueProvider.of(KEY_RESOURCE_NAME));
    Map<String, List<PrioDataSharePacket>> inputDataSharePackets =
        seedDatabaseAndReturnEntryVal(db);

    try {
      IngestionPipeline.runIngestionPipeline(options, flags);
    } finally {
      cleanUpDb();
      // Allow time for delete to execute.
      TimeUnit.SECONDS.sleep(15);
    }

    Map<String, List<PrioDataSharePacket>> actualDataSharepackets = readOutput();
    for (Map.Entry<String, List<PrioDataSharePacket>> entry : actualDataSharepackets.entrySet()) {
      Assert.assertTrue(
          "Output contains data which is not present in input",
          inputDataSharePackets.containsKey(entry.getKey()));
      comparePrioDataSharePacket(entry.getValue().get(0), inputDataSharePackets.get(entry.getKey()).get(0));
      comparePrioDataSharePacket(entry.getValue().get(1), inputDataSharePackets.get(entry.getKey()).get(1));
      // checkSuccessfulFork(forkedSharesFilePrefixes);
    }
  }

  private static void cleanUpDb() {

    listDocReference.forEach(DocumentReference::delete);
  }

  private static FirestoreClient getFirestoreClient()
      throws IOException {
    FirestoreSettings settings =
        FirestoreSettings.newBuilder().setCredentialsProvider(FixedCredentialsProvider.create(
            GoogleCredentials.getApplicationDefault())).build();
    return FirestoreClient.create(settings);
  }

  private Map<String, List<PrioDataSharePacket>> readOutput() throws IOException {
    Map<String, List<PrioDataSharePacket>> result = new HashMap<>();
    Stream<Path> pathsPha = Files.walk(Paths.get(tmpFolderPha.getRoot().getPath()));
    Stream<Path> pathsFac = Files.walk(Paths.get(tmpFolderFac.getRoot().getPath()));
    List<Path> pathListPha = pathsPha.filter(Files::isRegularFile).collect(Collectors.toList());
    List<Path> pathListFac = pathsFac.filter(Files::isRegularFile).collect(Collectors.toList());

    for (Path path : pathListPha) {
      if (path.toString().endsWith(".avro")) {
        List<PrioDataSharePacket> packets =
            PrioSerializationHelper.deserializeDataSharePackets(path.toString());
        for(PrioDataSharePacket pac : packets){
          if(!result.containsKey(pac.getUuid().toString())) {
            result.put(pac.getUuid().toString(), new ArrayList<>());
          }
          result.get(pac.getUuid().toString()).add(pac);
        }
      }
    }

    for (Path path : pathListFac) {
      if (path.toString().endsWith(".avro")) {
        List<PrioDataSharePacket> packets =
            PrioSerializationHelper.deserializeDataSharePackets(path.toString());
        for(PrioDataSharePacket pac : packets){
          //should not check for existance as facilitator and pha should have same key
          result.get(pac.getUuid().toString()).add(pac);
        }
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
   * Creates test-users collection and adds sample documents to test queries. Returns entry value in
   * form of {@link Map<String, PrioDataSharePacket>}.
   * @return
   */
  private static Map<String, List<PrioDataSharePacket>> seedDatabaseAndReturnEntryVal(
      Firestore db)
      throws ExecutionException, InterruptedException, IOException {
    // Adding a wait here to give the Firestore instance time to initialize before attempting
    // to connect.
    TimeUnit.SECONDS.sleep(1);

    WriteBatch batch = db.batch();

    Map<String, Object> docData = new HashMap<>();
    for (int i = 1; i <= 2; i++) {
      docData.put("id", "id" + i);
      docData.put(DataShare.PAYLOAD, getSamplePayload("uuid" + i, CREATION_TIME));
      docData.put(DataShare.SIGNATURE, "signature");
      docData.put(DataShare.CERT_CHAIN, Arrays.asList("cert1", "cert2"));
      DocumentReference reference = db.collection(TEST_COLLECTION_NAME)
          .document("random-assortment-" + i)
          .collection(formatDateTime(CREATION_TIME))
          .document("doc" + i);
      batch.set(reference, docData);
      listDocReference.add(reference);
    }

    ApiFuture<List<WriteResult>> future = batch.commit();
    // future.get() blocks on batch commit operation
    future.get();

    FirestoreClient client = getFirestoreClient();

    Map<String, List<PrioDataSharePacket>> dataShareByUuid = new HashMap<>();
    for (DocumentReference reference : listDocReference) {
      Document doc =
          client.getDocument(
              GetDocumentRequest.newBuilder()
                  .setName(
                      "projects/"
                          + FIREBASE_PROJECT_ID
                          + "/databases/(default)/documents/"
                          + reference.getPath())
                  .build());
      DataShare dataShare = DataShare.from(doc);
      List<EncryptedShare> encryptedDataShares = dataShare.getEncryptedDataShares();
      List<PrioDataSharePacket> splitDataShares = new ArrayList<>();
      for (EncryptedShare entry : encryptedDataShares) {
        splitDataShares.add(
            PrioDataSharePacket.newBuilder()
                .setEncryptionKeyId(entry.getEncryptionKeyId())
                .setEncryptedPayload(ByteBuffer.wrap(entry.getEncryptedPayload()))
                .setRPit(dataShare.getRPit())
                .setUuid(dataShare.getUuid())
                .build());
      }

      dataShareByUuid.put(dataShare.getUuid(), splitDataShares);
    }

    client.shutdown();
    int maxWait = 3;
    int wait = 1;
    while (client.awaitTermination(1000,TimeUnit.MILLISECONDS) == false) {
      if (wait++ == maxWait) break;
    };

    return dataShareByUuid;
  }

  private static void comparePrioDataSharePacket(
      PrioDataSharePacket first, PrioDataSharePacket second) {
    Assert.assertEquals(first.getUuid().toString(), second.getUuid().toString());
    Assert.assertEquals(first.getEncryptedPayload().toString(), second.getEncryptedPayload().toString());
    Assert.assertEquals(
        first.getEncryptionKeyId().toString(), second.getEncryptionKeyId().toString());
  }

  private static Map<String, Object> getSamplePayload(String uuid, long timestampSeconds) {
    Map<String, Object> samplePayload = new HashMap<>();

    Map<String, Object> samplePrioParams = new HashMap<>();
    samplePrioParams.put(DataShare.PRIME, 4293918721L);
    samplePrioParams.put(DataShare.BINS, 2L);
    samplePrioParams.put(DataShare.EPSILON, 5.2933D);
    samplePrioParams.put(DataShare.NUMBER_OF_SERVERS_FIELD, 2L);
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

  /*
   *  Within each fork, all packets with the same UUID should have unique encryption key Ids and encrypted payloads.
   *  The remaining fields (i.e. r_PIT)) should remain the same. This function ensures that this is the case.
   */
  private void checkSuccessfulFork(List<String> forkedSharesPrefixes) throws IOException {
    Stream<Path> paths = Files.walk(Paths.get(tmpFolderPha.getRoot().getPath()));
    List<Path> pathList = paths.filter(Files::isRegularFile).collect(Collectors.toList());
    List<List<PrioDataSharePacket>> forkedDataShares = new ArrayList<>();
    for (Path path : pathList) {
      for (String forkedSharesPrefix : forkedSharesPrefixes) {
        if (path.toString().startsWith(forkedSharesPrefix) && path.toString().endsWith(".avro")) {
          forkedDataShares.add(PrioSerializationHelper.deserializeDataSharePackets(path.toString()));
        }
      }
    }

    Map<String, Set<String>> uuidToKeyIds = new HashMap<>();
    Map<String, Set<String>> uuidToPayloads = new HashMap<>();

    // Key: UUID, Value: data share packet. This map is initialized with a single fork's packets and
    // used to ensure
    // that corresponding fields in other forks' packets are equivalent where expected.
    Map<String, PrioDataSharePacket> packetsToCompare = new HashMap<>();
    if (!forkedDataShares.isEmpty()) {
      for (PrioDataSharePacket packet : forkedDataShares.get(0)) {
        String uuid = packet.getUuid().toString();
        packetsToCompare.put(uuid, packet);
        Set<String> uuidKeys = new HashSet<>(Arrays.asList(packet.getEncryptionKeyId().toString()));
        uuidToKeyIds.put(uuid, uuidKeys);
        Set<String> uuidPayloads =
            new HashSet<>(Arrays.asList(packet.getEncryptedPayload().toString()));
        uuidToPayloads.put(uuid, uuidPayloads);
      }
    }

    for (int i = 1; i < forkedDataShares.size(); i++) {
      List<PrioDataSharePacket> packets = forkedDataShares.get(i);
      Assert.assertEquals(
          "Number of data shares is not equal in each fork.",
          packetsToCompare.size(),
          packets.size());
      for (PrioDataSharePacket packet : packets) {
        String uuid = packet.getUuid().toString();
        Assert.assertTrue(
            "UUID '" + uuid + "' does not appear in each fork.",
            packetsToCompare.containsKey(uuid));

        PrioDataSharePacket comparePacket = packetsToCompare.get(uuid);
        Assert.assertEquals(comparePacket.getRPit(), packet.getRPit());
        Assert.assertEquals(
            comparePacket.getVersionConfiguration(), packet.getVersionConfiguration());
        Assert.assertEquals(comparePacket.getDeviceNonce(), packet.getDeviceNonce());

        Set<String> uuidKeyIds = uuidToKeyIds.get(uuid);
        uuidKeyIds.add(packet.getEncryptionKeyId().toString());
        uuidToKeyIds.put(uuid, uuidKeyIds);
        Set<String> uuidPayloads = uuidToPayloads.get(uuid);
        uuidPayloads.add(packet.getEncryptionKeyId().toString());
        uuidToPayloads.put(uuid, uuidPayloads);
      }
    }

    // Check that each encryption key and payload associated with a UUID is unique.
    boolean allKeyIdsAreUnique = true;
    String errorUuid = "";
    int expectedKeyIdCount = forkedSharesPrefixes.size();
    for (Map.Entry<String, Set<String>> entry : uuidToKeyIds.entrySet()) {
      if (entry.getValue().size() != expectedKeyIdCount) {
        allKeyIdsAreUnique = false;
        errorUuid = entry.getKey();
        break;
      }
    }
    Assert.assertTrue(
        "Number of unique encryption key IDs associated with UUID '"
            + errorUuid
            + "' does not match the number of forked files provided. ("
            + +uuidToKeyIds.get(errorUuid).size()
            + " ) vs ("
            + expectedKeyIdCount
            + ").",
        allKeyIdsAreUnique);

    boolean allPayloadsAreUnique = true;
    int expectedPayloadCount = forkedSharesPrefixes.size();
    for (Map.Entry<String, Set<String>> entry : uuidToPayloads.entrySet()) {
      if (entry.getValue().size() != expectedPayloadCount) {
        allPayloadsAreUnique = false;
        errorUuid = entry.getKey();
        break;
      }
    }
    Assert.assertTrue(
        "Number of unique encrypted payloads associated with UUID '"
            + errorUuid
            + "' does not match the number of forked files provided. ("
            + +uuidToPayloads.get(errorUuid).size()
            + " ) vs ("
            + expectedPayloadCount
            + ").",
        allPayloadsAreUnique);
  }
}
