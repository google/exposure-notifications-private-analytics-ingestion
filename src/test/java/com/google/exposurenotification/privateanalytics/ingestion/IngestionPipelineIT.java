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

import static com.google.common.truth.Truth.assertThat;
import static com.google.exposurenotification.privateanalytics.ingestion.FirestoreConnector.formatDateTime;
import static org.junit.Assert.assertThrows;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPagedResponse;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.EncryptedShare;
import com.google.exposurenotification.privateanalytics.ingestion.FirestoreConnector.FirestoreReader;
import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.CreateDocumentRequest;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.GetDocumentRequest;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
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

  // Randomize document creation time to avoid collisions between simultaneously running tests.
  // FirestoreReader will query all documents with created times within one hour of this time.
  static final long CREATION_TIME = ThreadLocalRandom.current().nextLong(0L, 1602720000L);
  static final long DURATION = 10000L;
  static final String FIREBASE_PROJECT_ID = System.getenv("FIREBASE_PROJECT_ID");
  static final int MINIMUM_PARTICIPANT_COUNT = 1;
  // Randomize test collection name to avoid collisions between simultaneously running tests.
  static final String TEST_COLLECTION_NAME =
      "uuid" + UUID.randomUUID().toString().replace("-", "_");
  // TODO(amanraj): figure out way to not check this in
  static final String KEY_RESOURCE_NAME =
      "projects/appa-ingestion/locations/global/keyRings/appa-signature-key-ring/cryptoKeys/appa-signature-key/cryptoKeyVersions/1";

  static List<Document> documentList;

  @Rule
  public TemporaryFolder tmpFolderPha = new TemporaryFolder();
  @Rule
  public TemporaryFolder tmpFolderFac = new TemporaryFolder();

  public transient IngestionPipelineOptions testOptions =
      TestPipeline.testingPipelineOptions().as(IngestionPipelineOptions.class);

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.fromOptions(testOptions);

  @BeforeClass
  public static void setUp() {
    documentList = new ArrayList<>();
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
    options.setPHAOutput(phaFile.getAbsolutePath());
    options.setFacilitatorOutput(facilitatorFile.getAbsolutePath());
    options.setFirebaseProjectId(FIREBASE_PROJECT_ID);
    options.setMinimumParticipantCount(MINIMUM_PARTICIPANT_COUNT);
    options.setStartTime(CREATION_TIME);
    options.setDuration(DURATION);
    options.setKeyResourceName(KEY_RESOURCE_NAME);
    Map<String, List<PrioDataSharePacket>> inputDataSharePackets =
        seedDatabaseAndReturnEntryVal();

    try {
      IngestionPipeline.runIngestionPipeline(options);
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
      comparePrioDataSharePacket(entry.getValue().get(0),
          inputDataSharePackets.get(entry.getKey()).get(0));
      comparePrioDataSharePacket(entry.getValue().get(1),
          inputDataSharePackets.get(entry.getKey()).get(1));
      checkSuccessfulFork(forkedSharesFilePrefixes);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFirestoreDeleter_deletesDocs()
      throws InterruptedException, IOException, ExecutionException {
    File phaFile = tmpFolderPha.newFile();
    File facilitatorFile = tmpFolderFac.newFile();
    IngestionPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(IngestionPipelineOptions.class);
    List<String> forkedSharesFilePrefixes =
        Arrays.asList(
            getFilePath(phaFile.getAbsolutePath()), getFilePath(facilitatorFile.getAbsolutePath()));
    options.setDelete(true);
    options.setPHAOutput(phaFile.getAbsolutePath());
    options.setFacilitatorOutput(facilitatorFile.getAbsolutePath());
    options.setFirebaseProjectId(FIREBASE_PROJECT_ID);
    options.setMinimumParticipantCount(MINIMUM_PARTICIPANT_COUNT);
    options.setStartTime(CREATION_TIME);
    options.setDuration(DURATION);
    options.setKeyResourceName(KEY_RESOURCE_NAME);
    Map<String, List<PrioDataSharePacket>> inputDataSharePackets =
        seedDatabaseAndReturnEntryVal();

    PipelineResult result = IngestionPipeline.runIngestionPipeline(options);

    Map<String, List<PrioDataSharePacket>> actualDataSharepackets = readOutput();
    for (Map.Entry<String, List<PrioDataSharePacket>> entry : actualDataSharepackets.entrySet()) {
      Assert.assertTrue(
          "Output contains data which is not present in input",
          inputDataSharePackets.containsKey(entry.getKey()));
      comparePrioDataSharePacket(entry.getValue().get(0),
          inputDataSharePackets.get(entry.getKey()).get(0));
      comparePrioDataSharePacket(entry.getValue().get(1),
          inputDataSharePackets.get(entry.getKey()).get(1));
      checkSuccessfulFork(forkedSharesFilePrefixes);
    }
    // Wait for Pipeline to finish and assert processed documents have been deleted.
    TimeUnit.SECONDS.sleep(10);
    FirestoreClient client = getFirestoreClient();
    documentList.forEach(doc -> assertThrows(
        NotFoundException.class, () -> fetchDocumentFromFirestore(doc.getName(), client)));
    long documentsDeleted = result.metrics().queryMetrics(MetricsFilter.builder()
        .addNameFilter(MetricNameFilter.named(FirestoreConnector.class, "documentsDeleted"))
        .build()).getCounters().iterator().next().getCommitted();
    assertThat(documentsDeleted).isEqualTo(2);
    cleanUpParentResources(client);
    // Allow time for delete to execute.
    TimeUnit.SECONDS.sleep(10);
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFirestoreReader_readsCorrectNumberDocuments() {
    // Time at which the test collection with 10k docs was created.
    long startTimeFor10kDocs = 1603137600L;
    testOptions.setFirebaseProjectId(FIREBASE_PROJECT_ID);
    testOptions.setMinimumParticipantCount(MINIMUM_PARTICIPANT_COUNT);
    testOptions.setStartTime(startTimeFor10kDocs);
    testOptions.setDuration(DURATION);
    testOptions.setKeyResourceName(KEY_RESOURCE_NAME);

    PCollection<Long> numShares = testPipeline.apply(new FirestoreReader())
        .apply(Count.globally());

    PAssert.that(numShares).containsInAnyOrder(10000L);
    PipelineResult result = testPipeline.run();
    long partitionsCreated = result.metrics().queryMetrics(MetricsFilter.builder()
        .addNameFilter(MetricNameFilter.named(FirestoreConnector.class, "partitionCursors"))
        .build()).getCounters().iterator().next().getCommitted();
    // Assert that at least one partition was created. Number of partitions created is determined at
    // runtime, so we can't specify an exact number.
    assertThat(partitionsCreated).isGreaterThan(1);
  }

  private static Document fetchDocumentFromFirestore(String path, FirestoreClient client) {
    return client.getDocument(GetDocumentRequest.newBuilder().setName(path).build());
  }

  private static void cleanUpDb() throws IOException {
    FirestoreClient client = getFirestoreClient();
    documentList.forEach(doc -> client.deleteDocument(doc.getName()));
    cleanUpParentResources(client);
  }

  private static void cleanUpParentResources(FirestoreClient client) {
    ListDocumentsPagedResponse documents =
        client.listDocuments(
            ListDocumentsRequest.newBuilder()
                .setParent("projects/"
                    + FIREBASE_PROJECT_ID
                    + "/databases/(default)/documents")
                .setCollectionId(TEST_COLLECTION_NAME)
                .build());
    documents.iterateAll().forEach(document -> client.deleteDocument(document.getName()));
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
      if (path.toString().endsWith(BatchWriterFn.DATASHARE_PACKET_SUFFIX)) {
        List<PrioDataSharePacket> packets =
            PrioSerializationHelper.deserializeDataSharePackets(path.toString());
        for (PrioDataSharePacket pac : packets) {
          if (!result.containsKey(pac.getUuid().toString())) {
            result.put(pac.getUuid().toString(), new ArrayList<>());
          }
          result.get(pac.getUuid().toString()).add(pac);
        }
      }
    }

    for (Path path : pathListFac) {
      if (path.toString().endsWith(BatchWriterFn.DATASHARE_PACKET_SUFFIX)) {
        List<PrioDataSharePacket> packets =
            PrioSerializationHelper.deserializeDataSharePackets(path.toString());
        for (PrioDataSharePacket pac : packets) {
          // should not check for existence as facilitator and pha should have same key
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
   */
  private static Map<String, List<PrioDataSharePacket>> seedDatabaseAndReturnEntryVal()
      throws ExecutionException, InterruptedException, IOException {
    // Adding a wait here to give the Firestore instance time to initialize before attempting
    // to connect.
    TimeUnit.SECONDS.sleep(1);

    FirestoreClient client = getFirestoreClient();

    for (int i = 1; i <= 2; i++) {
      ArrayValue certs = ArrayValue.newBuilder()
          .addValues(Value.newBuilder().setStringValue("cert1").build())
          .addValues(Value.newBuilder().setStringValue("cert2").build())
          .build();
      Document doc =
          Document.newBuilder()
              .putFields(DataShare.SIGNATURE,
                  Value.newBuilder().setStringValue("signature").build())
              .putFields(DataShare.CERT_CHAIN, Value.newBuilder().setArrayValue(certs).build())
              .putFields(DataShare.PAYLOAD, Value.newBuilder().setMapValue(
                  MapValue.newBuilder().putAllFields(getSamplePayload("uuid" + i, CREATION_TIME))
                      .build()).build())
              .build();
      client.createDocument(CreateDocumentRequest.newBuilder()
          .setCollectionId(TEST_COLLECTION_NAME)
          .setDocumentId("testDoc" + i)
          .setParent("projects/"
              + FIREBASE_PROJECT_ID
              + "/databases/(default)/documents")
          .build());
      client.createDocument(CreateDocumentRequest.newBuilder()
          .setCollectionId(formatDateTime(CREATION_TIME))
          .setDocumentId("metric1")
          .setDocument(doc)
          .setParent("projects/"
              + FIREBASE_PROJECT_ID
              + "/databases/(default)/documents/" + TEST_COLLECTION_NAME + "/testDoc" + i)
          .build());
    }

    Map<String, List<PrioDataSharePacket>> dataShareByUuid = new HashMap<>();
    for (int i = 1; i <= 2; i++) {
      String docName =
          "projects/" + FIREBASE_PROJECT_ID + "/databases/(default)/documents/"
              + TEST_COLLECTION_NAME
              + "/testDoc" + i + "/"
              + formatDateTime(CREATION_TIME)
              + "/metric1";
      Document doc = fetchDocumentFromFirestore(docName, client);
      documentList.add(doc);
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
    while (client.awaitTermination(1000, TimeUnit.MILLISECONDS) == false) {
      if (wait++ == maxWait) {
        break;
      }
    }

    return dataShareByUuid;
  }

  private static void comparePrioDataSharePacket(
      PrioDataSharePacket first, PrioDataSharePacket second) {
    Assert.assertEquals(first.getUuid().toString(), second.getUuid().toString());
    Assert.assertEquals(first.getEncryptedPayload().toString(),
        second.getEncryptedPayload().toString());
    Assert.assertEquals(
        first.getEncryptionKeyId().toString(), second.getEncryptionKeyId().toString());
  }

  private static Map<String, Value> getSamplePayload(String uuid, long timestampSeconds) {
    Map<String, Value> samplePayload = new HashMap<>();

    Map<String, Value> prioParams = new HashMap<>();
    prioParams.put(DataShare.PRIME, Value.newBuilder().setIntegerValue(4293918721L).build());
    prioParams.put(DataShare.BINS, Value.newBuilder().setIntegerValue(2L).build());
    prioParams.put(DataShare.EPSILON, Value.newBuilder().setDoubleValue(5.2933D).build());
    prioParams
        .put(DataShare.NUMBER_OF_SERVERS_FIELD, Value.newBuilder().setIntegerValue(2L).build());
    prioParams.put(DataShare.HAMMING_WEIGHT, Value.newBuilder().setIntegerValue(1L).build());
    samplePayload.put(DataShare.PRIO_PARAMS,
        Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(prioParams).build())
            .build());

    List<Value> encryptedDataShares = new ArrayList<>();
    encryptedDataShares.add(
        Value.newBuilder().setMapValue(MapValue.newBuilder()
            .putFields(DataShare.ENCRYPTION_KEY_ID,
                Value.newBuilder().setStringValue("fakeEncryptionKeyId1").build())
            .putFields(DataShare.PAYLOAD, Value.newBuilder().setStringValue("fakePayload1").build())
            .build()).build());
    encryptedDataShares.add(
        Value.newBuilder().setMapValue(MapValue.newBuilder()
            .putFields(DataShare.ENCRYPTION_KEY_ID,
                Value.newBuilder().setStringValue("fakeEncryptionKeyId2").build())
            .putFields(DataShare.PAYLOAD, Value.newBuilder().setStringValue("fakePayload2").build())
            .build()).build());
    samplePayload.put(DataShare.ENCRYPTED_DATA_SHARES, Value.newBuilder()
        .setArrayValue(ArrayValue.newBuilder().addAllValues(encryptedDataShares).build()).build());

    samplePayload.put(DataShare.CREATED,
        Value.newBuilder()
            .setTimestampValue(
                com.google.protobuf.Timestamp.newBuilder().setSeconds(timestampSeconds).build())
            .build());
    samplePayload.put(DataShare.UUID, Value.newBuilder().setStringValue(uuid).build());

    return samplePayload;
  }

  /*
   *  Within each fork, all packets with the same UUID should have unique encryption key Ids and encrypted payloads.
   *  The remaining fields (i.e. r_PIT, device_nonce)) should remain the same. This function ensures that this is the case.
   */
  private void checkSuccessfulFork(List<String> forkedSharesPrefixes) throws IOException {
    Stream<Path> paths = Files.walk(Paths.get(tmpFolderPha.getRoot().getPath()));
    List<Path> pathList = paths.filter(Files::isRegularFile).collect(Collectors.toList());
    List<List<PrioDataSharePacket>> forkedDataShares = new ArrayList<>();
    for (String forkedSharesPrefix: forkedSharesPrefixes) {
      for (Path path: pathList) {
        if (path.toString().startsWith(forkedSharesPrefix) && path.toString().endsWith(BatchWriterFn.DATASHARE_PACKET_SUFFIX)) {
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
  }
}
