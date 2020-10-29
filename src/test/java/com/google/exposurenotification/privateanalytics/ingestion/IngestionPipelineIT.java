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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPagedResponse;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.cloud.kms.v1.Digest;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.DataShareMetadata;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.EncryptedShare;
import com.google.exposurenotification.privateanalytics.ingestion.FirestoreConnector.FirestoreReader;
import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.CreateDocumentRequest;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.GetDocumentRequest;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.abetterinternet.prio.v1.PrioBatchSignature;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.abetterinternet.prio.v1.PrioIngestionHeader;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link IngestionPipeline}. */
@RunWith(JUnit4.class)
public class IngestionPipelineIT {

  // Randomize document creation time to avoid collisions between simultaneously running tests.
  // FirestoreReader will query all documents with created times within one hour of this time.
  static final long CREATION_TIME = ThreadLocalRandom.current().nextLong(0L, 1602720000L);
  static final long DURATION = 10800L;
  static final String FIREBASE_PROJECT_ID = System.getenv("FIREBASE_PROJECT_ID");
  static final int MINIMUM_PARTICIPANT_COUNT = 1;
  // Randomize test collection name to avoid collisions between simultaneously running tests.
  static final String TEST_COLLECTION_NAME =
      "uuid" + UUID.randomUUID().toString().replace("-", "_");
  // TODO(amanraj): figure out way to not check this in
  static final String KEY_RESOURCE_NAME =
      "projects/appa-ingestion/"
          + "locations/global/"
          + "keyRings/appa-signature-key-ring/"
          + "cryptoKeys/appa-signature-key/"
          + "cryptoKeyVersions/1";

  // Default ingestion header fields
  static final Long DEFAULT_PRIME = 4293918721L;
  static final Long DEFAULT_BINS = 2L;
  static final Double DEFAULT_EPSILON = 5.2933D;
  static final Long DEFAULT_NUM_SERVERS = 2L;
  static final Long DEFAULT_HAMMING_WEIGHT = 1L;

  static final String STATE_ABBR = "NY";
  static List<Document> documentList;
  static FirestoreClient client;

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

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
    FirestoreConnector.shutdownFirestoreClient(client);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIngestionPipeline() throws Exception {
    IngestionPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(IngestionPipelineOptions.class);
    String phaDir =
        tmpFolder.newFolder("testIngestionPipeline/pha/" + STATE_ABBR).getAbsolutePath();
    String facDir =
        tmpFolder.newFolder("testIngestionPipeline/facilitator/" + STATE_ABBR).getAbsolutePath();
    options.setPHAOutput(phaDir);
    options.setFacilitatorOutput(facDir);
    options.setFirebaseProjectId(FIREBASE_PROJECT_ID);
    options.setMinimumParticipantCount(MINIMUM_PARTICIPANT_COUNT);
    options.setStartTime(CREATION_TIME);
    options.setDuration(DURATION);
    options.setKeyResourceName(KEY_RESOURCE_NAME);
    options.setDeviceAttestation(false);
    Map<String, List<PrioDataSharePacket>> inputDataSharePackets = seedDatabaseAndReturnEntryVal();

    try {
      PipelineResult result = IngestionPipeline.runIngestionPipeline(options);
      result.waitUntilFinish();
    } finally {
      cleanUpDb();
    }

    Map<String, List<PrioDataSharePacket>> actualDataSharepackets =
        readOutputShares(phaDir, facDir);
    for (Map.Entry<String, List<PrioDataSharePacket>> entry : actualDataSharepackets.entrySet()) {
      Assert.assertTrue(
          "Output contains data which is not present in input",
          inputDataSharePackets.containsKey(entry.getKey()));
      comparePrioDataSharePacket(
          entry.getValue().get(0), inputDataSharePackets.get(entry.getKey()).get(0));
      comparePrioDataSharePacket(
          entry.getValue().get(1), inputDataSharePackets.get(entry.getKey()).get(1));
    }
    checkSuccessfulFork(phaDir, facDir);
    verifyBatchOutput(phaDir);
    verifyBatchOutput(facDir);
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

    PCollection<Long> numShares = testPipeline.apply(new FirestoreReader()).apply(Count.globally());

    PAssert.that(numShares).containsInAnyOrder(10000L);
    PipelineResult result = testPipeline.run();
    long partitionsCreated =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(FirestoreConnector.class, "partitionCursors"))
                    .build())
            .getCounters()
            .iterator()
            .next()
            .getCommitted();
    // Assert that at least one partition was created. Number of partitions created is determined at
    // runtime, so we can't specify an exact number.
    assertThat(partitionsCreated).isGreaterThan(1);
  }

  private static Document fetchDocumentFromFirestore(String path, FirestoreClient client) {
    return client.getDocument(GetDocumentRequest.newBuilder().setName(path).build());
  }

  private static void cleanUpDb() throws IOException {
    documentList.forEach(doc -> client.deleteDocument(doc.getName()));
    cleanUpParentResources(client);
  }

  private static void cleanUpParentResources(FirestoreClient client) {
    ListDocumentsPagedResponse documents =
        client.listDocuments(
            ListDocumentsRequest.newBuilder()
                .setParent("projects/" + FIREBASE_PROJECT_ID + "/databases/(default)/documents")
                .setCollectionId(TEST_COLLECTION_NAME)
                .build());
    documents.iterateAll().forEach(document -> client.deleteDocument(document.getName()));
  }

  private static FirestoreClient getFirestoreClient() throws IOException {
    FirestoreSettings settings =
        FirestoreSettings.newBuilder()
            .setCredentialsProvider(
                FixedCredentialsProvider.create(GoogleCredentials.getApplicationDefault()))
            .build();
    return FirestoreClient.create(settings);
  }

  private Map<String, List<PrioDataSharePacket>> readOutputShares(String phaDir, String facDir)
      throws IOException, IllegalAccessException, InstantiationException {
    Map<String, List<PrioDataSharePacket>> result = new HashMap<>();
    List<PrioDataSharePacket> allPackets = new ArrayList<>();
    allPackets.addAll(getSharesInFolder(phaDir));
    allPackets.addAll(getSharesInFolder(facDir));
    for (PrioDataSharePacket packet : allPackets) {
      if (!result.containsKey(packet.getUuid().toString())) {
        result.put(packet.getUuid().toString(), new ArrayList<>());
      }
      result.get(packet.getUuid().toString()).add(packet);
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

    for (int i = 1; i <= 2; i++) {
      ArrayValue certs =
          ArrayValue.newBuilder()
              .addValues(
                  Value.newBuilder()
                      .setStringValue(
                          "MIICyjCCAm+gAwIBAgIBATAKBggqhkjOPQQDAjCBiDELMAkGA1UEBhMCVVMxEzARBgNVBAgMCkNhbGlmb3JuaWExFTATBgNVBAoMDEdvb2dsZSwgSW5jLjEQMA4GA1UECwwHQW5kcm9pZDE7MDkGA1UEAwwyQW5kcm9pZCBLZXlzdG9yZSBTb2Z0d2FyZSBBdHRlc3RhdGlvbiBJbnRlcm1lZGlhdGUwHhcNNzAwMTAxMDAwMDAwWhcNMjAxMDI2MDQyNDExWjAfMR0wGwYDVQQDDBRBbmRyb2lkIEtleXN0b3JlIEtleTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABFmB6pr+Vx3tRG5IbJmr5svDCVzN6Vj5umTvqX2zNnlX87NbSWa5+sO4vM/4UXhzBHilO9iCSgk0kqnmcV/kcVmjggEwMIIBLDALBgNVHQ8EBAMCB4AwgfsGCisGAQQB1nkCAREEgewwgekCAQIKAQACAQEKAQEEIArEEV+M4a4CIbE4ZI9LnZ+I3XNBCOwqLfGg8C7LbVDzBAAwgYa/gxEIAgYBdWMmPOu/gxIIAgYBdWMmPOu/hT0IAgYBdV4bVTi/hUVeBFwwWjE0MDIELGNvbS5nb29nbGUuYW5kcm9pZC5hcHBzLmV4cG9zdXJlbm90aWZpY2F0aW9uAgIifzEiBCBFC/YpsGWC0cg6i2+RVALAksa2I8XSSSCl8Vo9jBtuZTAuoQgxBgIBAgIBA6IDAgEDowQCAgEApQUxAwIBBKoDAgEBv4N3AgUAv4U+AwIBADAfBgNVHSMEGDAWgBQ//KzWGrE6noEguNUlHMVlux6RqTAKBggqhkjOPQQDAgNJADBGAiEAksHrOxEq1B1zmPCI0Pcj44l9uiBZVFipIj2XgIoO1pgCIQCGKcumj8qa3eGnwu5MqToIju0869uo0vUc/hyOvU1/aQ==")
                      .build())
              .addValues(
                  Value.newBuilder()
                      .setStringValue(
                          "MIICeDCCAh6gAwIBAgICEAEwCgYIKoZIzj0EAwIwgZgxCzAJBgNVBAYTAlVTMRMwEQYDVQQIDApDYWxpZm9ybmlhMRYwFAYDVQQHDA1Nb3VudGFpbiBWaWV3MRUwEwYDVQQKDAxHb29nbGUsIEluYy4xEDAOBgNVBAsMB0FuZHJvaWQxMzAxBgNVBAMMKkFuZHJvaWQgS2V5c3RvcmUgU29mdHdhcmUgQXR0ZXN0YXRpb24gUm9vdDAeFw0xNjAxMTEwMDQ2MDlaFw0yNjAxMDgwMDQ2MDlaMIGIMQswCQYDVQQGEwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEVMBMGA1UECgwMR29vZ2xlLCBJbmMuMRAwDgYDVQQLDAdBbmRyb2lkMTswOQYDVQQDDDJBbmRyb2lkIEtleXN0b3JlIFNvZnR3YXJlIEF0dGVzdGF0aW9uIEludGVybWVkaWF0ZTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABOueefhCY1msyyqRTImGzHCtkGaTgqlzJhP+rMv4ISdMIXSXSir+pblNf2bU4GUQZjW8U7ego6ZxWD7bPhGuEBSjZjBkMB0GA1UdDgQWBBQ//KzWGrE6noEguNUlHMVlux6RqTAfBgNVHSMEGDAWgBTIrel3TEXDo88NFhDkeUM6IVowzzASBgNVHRMBAf8ECDAGAQH/AgEAMA4GA1UdDwEB/wQEAwIChDAKBggqhkjOPQQDAgNIADBFAiBLipt77oK8wDOHri/AiZi03cONqycqRZ9pDMfDktQPjgIhAO7aAV229DLp1IQ7YkyUBO86fMy9Xvsiu+f+uXc/WT/7")
                      .build())
              .addValues(
                  Value.newBuilder()
                      .setStringValue(
                          "MIICizCCAjKgAwIBAgIJAKIFntEOQ1tXMAoGCCqGSM49BAMCMIGYMQswCQYDVQQGEwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNTW91bnRhaW4gVmlldzEVMBMGA1UECgwMR29vZ2xlLCBJbmMuMRAwDgYDVQQLDAdBbmRyb2lkMTMwMQYDVQQDDCpBbmRyb2lkIEtleXN0b3JlIFNvZnR3YXJlIEF0dGVzdGF0aW9uIFJvb3QwHhcNMTYwMTExMDA0MzUwWhcNMzYwMTA2MDA0MzUwWjCBmDELMAkGA1UEBhMCVVMxEzARBgNVBAgMCkNhbGlmb3JuaWExFjAUBgNVBAcMDU1vdW50YWluIFZpZXcxFTATBgNVBAoMDEdvb2dsZSwgSW5jLjEQMA4GA1UECwwHQW5kcm9pZDEzMDEGA1UEAwwqQW5kcm9pZCBLZXlzdG9yZSBTb2Z0d2FyZSBBdHRlc3RhdGlvbiBSb290MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE7l1ex+HA220Dpn7mthvsTWpdamguD/9/SQ59dx9EIm29sa/6FsvHrcV30lacqrewLVQBXT5DKyqO107sSHVBpKNjMGEwHQYDVR0OBBYEFMit6XdMRcOjzw0WEOR5QzohWjDPMB8GA1UdIwQYMBaAFMit6XdMRcOjzw0WEOR5QzohWjDPMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgKEMAoGCCqGSM49BAMCA0cAMEQCIDUho++LNEYenNVg8x1YiSBq3KNlQfYNns6KGYxmSGB7AiBNC/NR2TB8fVvaNTQdqEcbY6WFZTytTySn502vQX3xvw==")
                      .build())
              .build();
      Document doc =
          Document.newBuilder()
              .putFields(
                  DataShare.SIGNATURE,
                  Value.newBuilder()
                      .setStringValue(
                          "MEYCIQCkprsCPR34TAINZE+bfx+R1u96qKpvvWuLAQVlG/9ROQIhAIvRvSxor4fF0HHbolnfrA2rA5Cxor5W3HoTcQyS3bfM")
                      .build())
              .putFields(DataShare.CERT_CHAIN, Value.newBuilder().setArrayValue(certs).build())
              .putFields(
                  DataShare.PAYLOAD,
                  Value.newBuilder()
                      .setMapValue(
                          MapValue.newBuilder()
                              .putAllFields(getSamplePayload("uuid" + i, CREATION_TIME))
                              .build())
                      .build())
              .build();
      client.createDocument(
          CreateDocumentRequest.newBuilder()
              .setCollectionId(TEST_COLLECTION_NAME)
              .setDocumentId("testDoc" + i)
              .setParent("projects/" + FIREBASE_PROJECT_ID + "/databases/(default)/documents")
              .build());
      client.createDocument(
          CreateDocumentRequest.newBuilder()
              .setCollectionId(formatDateTime(CREATION_TIME))
              .setDocumentId("metric1")
              .setDocument(doc)
              .setParent(
                  "projects/"
                      + FIREBASE_PROJECT_ID
                      + "/databases/(default)/documents/"
                      + TEST_COLLECTION_NAME
                      + "/testDoc"
                      + i)
              .build());
    }

    Map<String, List<PrioDataSharePacket>> dataShareByUuid = new HashMap<>();
    for (int i = 1; i <= 2; i++) {
      String docName =
          "projects/"
              + FIREBASE_PROJECT_ID
              + "/databases/(default)/documents/"
              + TEST_COLLECTION_NAME
              + "/testDoc"
              + i
              + "/"
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
    return dataShareByUuid;
  }

  private static void comparePrioDataSharePacket(
      PrioDataSharePacket first, PrioDataSharePacket second) {
    Assert.assertEquals(first.getUuid().toString(), second.getUuid().toString());
    Assert.assertEquals(
        first.getEncryptedPayload().toString(), second.getEncryptedPayload().toString());
    Assert.assertEquals(
        first.getEncryptionKeyId().toString(), second.getEncryptionKeyId().toString());
  }

  private static Map<String, Value> getSamplePayload(String uuid, long timestampSeconds) {
    Map<String, Value> samplePayload = new HashMap<>();

    Map<String, Value> prioParams = new HashMap<>();
    prioParams.put(DataShare.PRIME, Value.newBuilder().setIntegerValue(DEFAULT_PRIME).build());
    prioParams.put(DataShare.BINS, Value.newBuilder().setIntegerValue(DEFAULT_BINS).build());
    prioParams.put(DataShare.EPSILON, Value.newBuilder().setDoubleValue(DEFAULT_EPSILON).build());
    prioParams.put(
        DataShare.NUMBER_OF_SERVERS_FIELD,
        Value.newBuilder().setIntegerValue(DEFAULT_NUM_SERVERS).build());
    prioParams.put(
        DataShare.HAMMING_WEIGHT,
        Value.newBuilder().setIntegerValue(DEFAULT_HAMMING_WEIGHT).build());
    samplePayload.put(
        DataShare.PRIO_PARAMS,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(prioParams).build())
            .build());

    List<Value> encryptedDataShares = new ArrayList<>();
    encryptedDataShares.add(
        Value.newBuilder()
            .setMapValue(
                MapValue.newBuilder()
                    .putFields(
                        DataShare.ENCRYPTION_KEY_ID,
                        Value.newBuilder().setStringValue("fakeEncryptionKeyId1").build())
                    .putFields(
                        DataShare.PAYLOAD,
                        Value.newBuilder().setStringValue("fakePayload1").build())
                    .build())
            .build());
    encryptedDataShares.add(
        Value.newBuilder()
            .setMapValue(
                MapValue.newBuilder()
                    .putFields(
                        DataShare.ENCRYPTION_KEY_ID,
                        Value.newBuilder().setStringValue("fakeEncryptionKeyId2").build())
                    .putFields(
                        DataShare.PAYLOAD,
                        Value.newBuilder().setStringValue("fakePayload2").build())
                    .build())
            .build());
    samplePayload.put(
        DataShare.ENCRYPTED_DATA_SHARES,
        Value.newBuilder()
            .setArrayValue(ArrayValue.newBuilder().addAllValues(encryptedDataShares).build())
            .build());

    samplePayload.put(
        DataShare.CREATED,
        Value.newBuilder()
            .setTimestampValue(
                com.google.protobuf.Timestamp.newBuilder().setSeconds(timestampSeconds).build())
            .build());
    samplePayload.put(DataShare.UUID, Value.newBuilder().setStringValue(uuid).build());
    samplePayload.put(
        DataShare.SCHEMA_VERSION,
        Value.newBuilder().setIntegerValue(DataShare.LATEST_SCHEMA_VERSION).build());

    return samplePayload;
  }

  /*
   *  Within each fork, all packets with the same UUID should have unique encryption key Ids. On the other hand, certain
   *  fields should remain the same (r_PIT, device_nonce, version config) This function ensures that this is the case.
   */
  private void checkSuccessfulFork(String phaDir, String facDir)
      throws IOException, IllegalAccessException, InstantiationException {
    List<PrioDataSharePacket> phaShares = getSharesInFolder(phaDir);
    Assert.assertTrue("No shares found in phaFolder", phaShares.size() > 0);
    Map<String, PrioDataSharePacket> phaUuidToPacket = new HashMap<>();
    for (PrioDataSharePacket packet : phaShares) {
      phaUuidToPacket.put(packet.getUuid().toString(), packet);
    }

    List<PrioDataSharePacket> facShares = getSharesInFolder(facDir);
    Assert.assertTrue("No shares found in facilitatorFolder", facShares.size() > 0);
    Assert.assertEquals(
        "Number of data shares in each fork is not equal. \nPHA Shares Count: "
            + phaShares.size()
            + "\nFacilitator shares count: "
            + facShares.size(),
        phaShares.size(),
        facShares.size());

    for (PrioDataSharePacket facPacket : facShares) {
      String uuid = facPacket.getUuid().toString();
      Assert.assertTrue(
          "UUID '" + uuid + "' appears in the facilitator fork but not in the PHA fork.",
          phaUuidToPacket.containsKey(uuid));
      PrioDataSharePacket phaPacket = phaUuidToPacket.get(uuid);
      Assert.assertEquals(phaPacket.getRPit(), facPacket.getRPit());
      Assert.assertEquals(phaPacket.getVersionConfiguration(), facPacket.getVersionConfiguration());
      Assert.assertEquals(phaPacket.getDeviceNonce(), facPacket.getDeviceNonce());
      Assert.assertFalse(
          phaPacket
              .getEncryptionKeyId()
              .toString()
              .equals(facPacket.getEncryptionKeyId().toString()));
    }
  }

  private void verifyBatchOutput(String batchFolder)
      throws IOException, IllegalAccessException, InstantiationException,
          java.security.NoSuchAlgorithmException, InterruptedException {
    Stream<Path> paths = Files.walk(Paths.get(batchFolder));
    List<Path> pathList = paths.filter(Files::isRegularFile).collect(Collectors.toList());
    int verifiedBatchesCount = 0;
    for (Path path : pathList) {
      if (!path.toString().endsWith(BatchWriterFn.DATASHARE_PACKET_SUFFIX)) {
        continue;
      }
      // Step 1: Verify the path format.
      String invalidPathMessage =
          "Expected file path to end in: "
              + "{2-letter state abbreviation}/google/{metricName}/YYYY/mm/dd/HH/MM/{batchId}{.batch, .batch.sig, or .batch.avro}"
              + "\nActual path: "
              + path.toString();
      Assert.assertTrue(
          invalidPathMessage,
          path.toString()
              .matches(
                  ".*/google/[a-z0-9_\\-]+(/\\d{4}/)(\\d{2}/){4}[a-zA-Z0-9\\-]+\\.batch($|.sig$|.avro$)"));
      String batchUuid =
          path.getFileName().toString().replace(BatchWriterFn.DATASHARE_PACKET_SUFFIX, "");
      Assert.assertEquals(
          "Invalid length of batchId.\n" + invalidPathMessage, 36, batchUuid.length());

      // Step 2: Verify the header file associated with this data share batch.
      byte[] packetBatchBytes = Files.readAllBytes(path);
      MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
      byte[] packetBatchHash = sha256.digest(packetBatchBytes);
      Digest digest = Digest.newBuilder().setSha256(ByteString.copyFrom(packetBatchHash)).build();
      Pattern r = Pattern.compile("/google/(.*)/\\d{4}");
      Matcher m = r.matcher(path.toString());
      String metricName = "";
      if (m.find()) {
        metricName = m.group(0);
      } else {
        Assert.fail(invalidPathMessage);
      }
      PrioIngestionHeader expectedHeader =
          PrioSerializationHelper.createHeader(
              DataShareMetadata.builder()
                  .setBins(DEFAULT_BINS.intValue())
                  .setEpsilon(DEFAULT_EPSILON)
                  .setPrime(DEFAULT_PRIME.longValue())
                  .setNumberOfServers(DEFAULT_NUM_SERVERS.intValue())
                  .setHammingWeight(DEFAULT_HAMMING_WEIGHT.intValue())
                  .setMetricName(metricName)
                  .build(),
              digest,
              UUID.fromString(batchUuid),
              CREATION_TIME,
              DURATION);
      String expectedHeaderFilename =
          path.getParent().toString() + "/" + batchUuid + BatchWriterFn.INGESTION_HEADER_SUFFIX;
      Assert.assertTrue(
          "Missing header file associated witch batch id " + batchUuid,
          new File(expectedHeaderFilename).isFile());
      PrioIngestionHeader actualHeader =
          PrioSerializationHelper.deserializeRecords(
                  PrioIngestionHeader.class, expectedHeaderFilename)
              .get(0);
      Assert.assertEquals(expectedHeader.getBins(), actualHeader.getBins());
      Assert.assertEquals(expectedHeader.getName(), actualHeader.getName());
      Assert.assertEquals(expectedHeader.getBatchUuid(), actualHeader.getBatchUuid());
      Assert.assertEquals(expectedHeader.getEpsilon(), actualHeader.getEpsilon(), .00001);
      Assert.assertEquals(expectedHeader.getPrime(), actualHeader.getPrime());
      Assert.assertEquals(expectedHeader.getNumberOfServers(), actualHeader.getNumberOfServers());
      Assert.assertEquals(expectedHeader.getHammingWeight(), actualHeader.getHammingWeight());
      Assert.assertEquals(expectedHeader.getBatchStartTime(), actualHeader.getBatchStartTime());
      Assert.assertEquals(expectedHeader.getBatchEndTime(), actualHeader.getBatchEndTime());
      Assert.assertEquals(expectedHeader.getPacketFileDigest(), actualHeader.getPacketFileDigest());

      // Step 3: Verify the signature file associated with this data share batch.
      String expectedSignatureFilename =
          path.getParent().toString() + "/" + batchUuid + BatchWriterFn.HEADER_SIGNATURE_SUFFIX;
      Assert.assertTrue(
          "Missing signature file associated witch batch id " + batchUuid,
          new File(expectedSignatureFilename).isFile());
      PrioBatchSignature actualSignature =
          PrioSerializationHelper.deserializeRecords(
                  PrioBatchSignature.class, expectedSignatureFilename)
              .get(0);
      Assert.assertEquals(KEY_RESOURCE_NAME, actualSignature.getKeyIdentifier().toString());

      verifiedBatchesCount += 1;
    }

    Assert.assertTrue("No valid batch output found in this folder.", verifiedBatchesCount > 0);
  }

  private List<PrioDataSharePacket> getSharesInFolder(String folder)
      throws IOException, IllegalAccessException, InstantiationException {
    Stream<Path> paths = Files.walk(Paths.get(folder));
    List<Path> pathList = paths.filter(Files::isRegularFile).collect(Collectors.toList());
    List<PrioDataSharePacket> allDataSharesInFolder = new ArrayList<>();
    for (Path path : pathList) {
      if (path.toString().endsWith(BatchWriterFn.DATASHARE_PACKET_SUFFIX)) {
        allDataSharesInFolder.addAll(
            PrioSerializationHelper.deserializeRecords(PrioDataSharePacket.class, path.toString()));
      }
    }
    return allDataSharesInFolder;
  }
}
