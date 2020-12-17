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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPagedResponse;
import com.google.cloud.firestore.v1.FirestoreSettings;
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
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.abetterinternet.prio.v1.PrioBatchSignature;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.abetterinternet.prio.v1.PrioIngestionHeader;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.DistributionResult;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration tests for {@link IngestionPipeline}. */
@RunWith(JUnit4.class)
public class IngestionPipelineIT {

  private static final Logger LOG = LoggerFactory.getLogger(IngestionPipelineIT.class);

  // Randomize document creation time to avoid collisions between simultaneously running tests.
  // FirestoreReader will query all documents with created times within one hour of this time.
  static final long CREATION_TIME = ThreadLocalRandom.current().nextLong(0L, 1602720000L);
  static final long DURATION = 10800L;
  static final String PROJECT = System.getenv("PROJECT");
  // Randomize test collection name to avoid collisions between simultaneously running tests.
  static final String TEST_COLLECTION_NAME =
      "uuid" + UUID.randomUUID().toString().replace("-", "_");
  static final String KEY_RESOURCE_NAME = System.getenv("KEY_RESOURCE_NAME");

  // Default ingestion header fields
  static final Long DEFAULT_BINS = 2L;
  static final Double DEFAULT_EPSILON = 5.2933D;
  static final Long DEFAULT_HAMMING_WEIGHT = 1L;

  static final String STATE_ABBR = "NY";
  static List<String> documentList;
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
    cleanUpDb();
    FirestoreConnector.shutdownFirestoreClient(client);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIngestionPipeline() throws Exception {
    String phaDir =
        tmpFolder.newFolder("testIngestionPipeline/pha/" + STATE_ABBR).getAbsolutePath();
    String facDir =
        tmpFolder.newFolder("testIngestionPipeline/facilitator/" + STATE_ABBR).getAbsolutePath();

    testOptions.setPhaOutput(phaDir);
    testOptions.setFacilitatorOutput(facDir);
    testOptions.setStartTime(CREATION_TIME);
    testOptions.setProject(PROJECT);
    testOptions.setDuration(DURATION);
    testOptions.setKeyResourceName(KEY_RESOURCE_NAME);
    testOptions.setBatchSize(1L);
    testOptions.setDeviceAttestation(false);

    int numDocs = 2;
    Map<String, List<PrioDataSharePacket>> inputDataSharePackets =
        seedDatabaseAndReturnEntryVal(numDocs);

    PipelineResult result = IngestionPipeline.runIngestionPipeline(testOptions);
    result.waitUntilFinish();

    compareSeededPacketsToActual(inputDataSharePackets, phaDir, facDir);
    checkSuccessfulFork(phaDir, facDir);
    verifyBatchOutput(phaDir);
    verifyBatchOutput(facDir);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIngestionPipelineAWS() throws Exception {
    testOptions.setStartTime(CREATION_TIME);
    testOptions.setProject(PROJECT);
    testOptions.setDuration(DURATION);
    testOptions.setKeyResourceName(KEY_RESOURCE_NAME);
    testOptions.setDeviceAttestation(false);
    testOptions.setAwsRegion("global");
    LOG.info("Project: " + testOptions.getProject());
    LOG.info("AWS Region: " + testOptions.getAwsRegion());
    int numDocs = 2;
    Map<String, List<PrioDataSharePacket>> inputDataSharePackets =
        seedDatabaseAndReturnEntryVal(numDocs);
    final String TEST_IDENTIFIER = "testIngestionPipelineAWS_" + UUID.randomUUID().toString();
    String phaDir = "s3://federation-pha/" + TEST_IDENTIFIER + "/pha/" + STATE_ABBR;
    String facDir = "s3://federation-facilitator/" + TEST_IDENTIFIER + "/fac/" + STATE_ABBR;
    testOptions.setPhaOutput(phaDir);
    testOptions.setPhaAwsBucketName("federation-pha");
    testOptions.setPhaAwsBucketRegion("us-east-2");
    testOptions.setPhaAwsBucketRole("arn:aws:iam::543928124548:role/AssumeRolePHA");

    testOptions.setFacilitatorOutput(facDir);
    testOptions.setFacilitatorAwsBucketName("federation-facilitator");
    testOptions.setFacilitatorAwsBucketRegion("us-east-2");
    testOptions.setFacilitatorAwsBucketRole("arn:aws:iam::543928124548:role/AssumeRoleFacilitator");
    PipelineResult result = IngestionPipeline.runIngestionPipeline(testOptions);
    result.waitUntilFinish();

    AWSFederatedAuthHelper.setupAWSAuth(
        testOptions, testOptions.getPhaAwsBucketRole(), testOptions.getPhaAwsBucketRegion());
    AmazonS3 s3ClientPha =
        AmazonS3ClientBuilder.standard()
            .withCredentials(testOptions.getAwsCredentialsProvider())
            .withRegion(testOptions.getPhaAwsBucketRegion())
            .build();

    List<String> phaObjectKeys =
        getTestObjKeys(s3ClientPha, testOptions.getPhaAwsBucketName(), TEST_IDENTIFIER);
    Assert.assertEquals(
        "Expected 3 object keys (header, signature, packet batch)", 3, phaObjectKeys.size());

    AWSFederatedAuthHelper.setupAWSAuth(
        testOptions,
        testOptions.getFacilitatorAwsBucketRole(),
        testOptions.getFacilitatorAwsBucketRegion());
    AmazonS3 s3ClientFac =
        AmazonS3ClientBuilder.standard()
            .withCredentials(testOptions.getAwsCredentialsProvider())
            .withRegion(testOptions.getFacilitatorAwsBucketRegion())
            .build();
    List<String> facObjectKeys =
        getTestObjKeys(s3ClientFac, testOptions.getFacilitatorAwsBucketName(), TEST_IDENTIFIER);

    Assert.assertEquals(
        "Expected 3 object keys (header, signature, packet batch)", 3, facObjectKeys.size());
    for (int i = 0; i < 3; i++) {
      String phaObjectKey = phaObjectKeys.get(i);
      String facObjectKey = facObjectKeys.get(i);
      Assert.assertTrue(
          phaObjectKey.endsWith(BatchWriterFn.HEADER_SIGNATURE_SUFFIX)
              || phaObjectKey.endsWith(BatchWriterFn.INGESTION_HEADER_SUFFIX)
              || phaObjectKey.endsWith(BatchWriterFn.DATASHARE_PACKET_SUFFIX));
      Assert.assertTrue(
          facObjectKey.endsWith(BatchWriterFn.HEADER_SIGNATURE_SUFFIX)
              || facObjectKey.endsWith(BatchWriterFn.INGESTION_HEADER_SUFFIX)
              || facObjectKey.endsWith(BatchWriterFn.DATASHARE_PACKET_SUFFIX));
    }
  }

  // Test the ingestion pipeline with device attestation enabled. This test checks that the device
  // attestation filters
  // shares with invalid signatures and/or certificates and allows shares with valid fields to pass
  // through.
  @Test
  @Category(NeedsRunner.class)
  public void testIngestionPipeline_deviceAttestationEnabled() throws Exception {
    IngestionPipelineOptions testOptions =
        TestPipeline.testingPipelineOptions().as(IngestionPipelineOptions.class);
    String phaDir =
        tmpFolder.newFolder("testDeviceAttestation/pha/" + STATE_ABBR).getAbsolutePath();
    String facDir =
        tmpFolder.newFolder("testDeviceAttestation/facilitator/" + STATE_ABBR).getAbsolutePath();
    long startTime = 1604039801L;
    testOptions.setPhaOutput(phaDir);
    testOptions.setFacilitatorOutput(facDir);
    testOptions.setStartTime(DeviceAttestationTest.CREATED_TIME);
    testOptions.setProject(PROJECT);
    testOptions.setDuration(3600L);
    testOptions.setGraceHoursBackwards(0L);
    testOptions.setGraceHoursForwards(0L);
    testOptions.setKeyResourceName(KEY_RESOURCE_NAME);
    testOptions.setBatchSize(1L);
    testOptions.setDeviceAttestation(true);

    List<Document> docs = new ArrayList<>();
    Map<String, Value> validDocFields = DeviceAttestationTest.getValidDocFields();
    Document validDoc = Document.newBuilder().putAllFields(validDocFields).build();
    docs.add(validDoc);

    Map<String, Value> fieldsWithInvalidSig = DeviceAttestationTest.getValidDocFields();
    fieldsWithInvalidSig.replace(
        DataShare.SIGNATURE, Value.newBuilder().setStringValue("invalidSignature").build());
    Document docWithInvalidSig = Document.newBuilder().putAllFields(fieldsWithInvalidSig).build();
    docs.add(docWithInvalidSig);

    Map<String, Value> fieldsWithInvalidCerts = DeviceAttestationTest.getValidDocFields();
    fieldsWithInvalidSig.replace(
        DataShare.CERT_CHAIN, Value.newBuilder().setStringValue("invalidSignature").build());
    fieldsWithInvalidCerts.put(
        DataShare.CERT_CHAIN,
        Value.newBuilder()
            .setArrayValue(
                ArrayValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("invalidCert1").build())
                    .addValues(Value.newBuilder().setStringValue("invalidCert2").build())
                    .addValues(Value.newBuilder().setStringValue("invalidCert3").build())
                    .addValues(Value.newBuilder().setStringValue("invalidCert4").build())
                    .build())
            .build());
    Document docWithInvalidCerts =
        Document.newBuilder().putAllFields(fieldsWithInvalidCerts).build();
    docs.add(docWithInvalidCerts);

    client.createDocument(
        CreateDocumentRequest.newBuilder()
            .setCollectionId(TEST_COLLECTION_NAME)
            .setDocumentId("testDoc")
            .setParent("projects/" + PROJECT + "/databases/(default)/documents")
            .build());

    for (int i = 0; i < docs.size(); i++) {
      client.createDocument(
          CreateDocumentRequest.newBuilder()
              .setCollectionId(formatDateTime(DeviceAttestationTest.CREATED_TIME))
              .setDocumentId("testDoc" + i)
              .setDocument(docs.get(i))
              .setParent(
                  "projects/"
                      + PROJECT
                      + "/databases/(default)/documents/"
                      + TEST_COLLECTION_NAME
                      + "/testDoc")
              .build());
    }

    PipelineResult result = IngestionPipeline.runIngestionPipeline(testOptions);
    result.waitUntilFinish();

    for (int i = 0; i < docs.size(); i++) {
      String docName =
          "projects/"
              + PROJECT
              + "/databases/(default)/documents/"
              + TEST_COLLECTION_NAME
              + "/testDoc"
              + "/"
              + formatDateTime(DeviceAttestationTest.CREATED_TIME)
              + "/testDoc"
              + i;
      Document doc = fetchDocumentFromFirestore(docName, client);
      documentList.add(doc.getName());
    }
    List<PrioDataSharePacket> phaShares = getSharesInFolder(phaDir, DeviceAttestationTest.UUID);
    // If the docs with invalid signatures/certificates were filtered, we expect only one share.
    // (from the valid doc)
    Assert.assertEquals(1, phaShares.size());
    List<PrioDataSharePacket> facShares = getSharesInFolder(facDir, DeviceAttestationTest.UUID);
    Assert.assertEquals(1, facShares.size());
    PrioDataSharePacket actualPhaShare = phaShares.get(0);
    PrioDataSharePacket actualFacShare = facShares.get(0);

    ByteBuffer expectedPhaPayload =
        ByteBuffer.wrap(
            Base64.getDecoder()
                .decode(
                    "BGi3cl2yoxXJsJkzWUMgf8GdcodIlt+0EGrSAkbh5e0KWgepJh4zIxY9Rg9BsaxqH6Qt12hRgvvnwvAEkE14fy05Gh99swkIHuArf4qznU7BTTdXUks3cx9agPFjf3hG9s2UF0zB22puOwjTF4Nvo91nTS/qMltBPDJhL9FkpM4zyBi7QbwfRFGTnOZVXgOdPoguqpOqNb4wD9t1LBh+xE22sJVWKUN+sC1Y70e5iboFVM84jabTo8aySmZp1UAPSMaYeYM="));
    ByteBuffer expectedFacPayload =
        ByteBuffer.wrap(
            Base64.getDecoder()
                .decode(
                    "BNWQJlfKoGMzsOSbwCgrg+go0v9GrHMKSIjZ/uLhCQMUTdDsa0VOWy7P1H9ptKRwxaT1UYHcJFc0vNIzf8QEujCh3fmaI4DU7yExsgnvLIv/Fl0clGclLy0UrfAnMIvSnQ17CcNzOt6MvjEwiwMwTQQ="));

    Assert.assertEquals(expectedPhaPayload, actualPhaShare.getEncryptedPayload());
    Assert.assertEquals(expectedFacPayload, actualFacShare.getEncryptedPayload());
    DistributionResult repeatedCertMetric =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(DeviceAttestation.class, "repeatedCerts-dist"))
                    .build())
            .getDistributions()
            .iterator()
            .next()
            .getCommitted();
    Assert.assertEquals(2, repeatedCertMetric.getCount());
    Assert.assertEquals(3, repeatedCertMetric.getSum());
    Assert.assertEquals(2, repeatedCertMetric.getMax());
    Assert.assertEquals(1, repeatedCertMetric.getMin());
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFirestoreReader_partitionsQueryAndReadsCorrectNumberDocuments()
      throws InterruptedException {
    testOptions.setStartTime(CREATION_TIME);
    testOptions.setDuration(DURATION);
    testOptions.setProject(PROJECT);
    testOptions.setKeyResourceName(KEY_RESOURCE_NAME);
    int numDocs = 200;
    seedDatabaseAndReturnEntryVal(numDocs);

    PCollection<Long> numShares =
        testPipeline.apply(new FirestoreReader(CREATION_TIME)).apply(Count.globally());

    PAssert.that(numShares).containsInAnyOrder((long) numDocs);
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

  private static void cleanUpDb() {
    documentList.forEach(docPath -> client.deleteDocument(docPath));
    cleanUpParentResources(client);
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
    allPackets.addAll(getSharesInFolder(phaDir, TEST_COLLECTION_NAME));
    allPackets.addAll(getSharesInFolder(facDir, TEST_COLLECTION_NAME));
    for (PrioDataSharePacket packet : allPackets) {
      if (!result.containsKey(packet.getUuid().toString())) {
        result.put(packet.getUuid().toString(), new ArrayList<>());
      }
      result.get(packet.getUuid().toString()).add(packet);
    }
    return result;
  }

  /**
   * Creates test-users collection and adds sample documents to test queries. Returns entry value in
   * form of {@link Map<String, PrioDataSharePacket>}.
   */
  private static Map<String, List<PrioDataSharePacket>> seedDatabaseAndReturnEntryVal(
      int numDocsToSeed) throws InterruptedException {
    // Adding a wait here to give the Firestore instance time to initialize before attempting
    // to connect.
    TimeUnit.SECONDS.sleep(1);

    for (int i = 1; i <= numDocsToSeed; i++) {
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
                              .putAllFields(
                                  getSamplePayload(TEST_COLLECTION_NAME + "-" + i, CREATION_TIME))
                              .build())
                      .build())
              .build();
      client.createDocument(
          CreateDocumentRequest.newBuilder()
              .setCollectionId(TEST_COLLECTION_NAME)
              .setDocumentId("testDoc" + i)
              .setParent("projects/" + PROJECT + "/databases/(default)/documents")
              .build());
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
              .build());
    }

    Map<String, List<PrioDataSharePacket>> dataShareByUuid = new HashMap<>();
    for (int i = 1; i <= numDocsToSeed; i++) {
      String docName =
          "projects/"
              + PROJECT
              + "/databases/(default)/documents/"
              + TEST_COLLECTION_NAME
              + "/testDoc"
              + i
              + "/"
              + formatDateTime(CREATION_TIME)
              + "/metric1";
      Document doc = fetchDocumentFromFirestore(docName, client);
      // Add document to documentList so that it is deleted in cleanup phase of test run.
      documentList.add(doc.getName());
      DataShare dataShare = DataShare.from(doc);
      List<EncryptedShare> encryptedDataShares = dataShare.getEncryptedDataShares();
      List<PrioDataSharePacket> splitDataShares = new ArrayList<>();
      for (EncryptedShare entry : encryptedDataShares) {
        splitDataShares.add(
            PrioDataSharePacket.newBuilder()
                .setEncryptedPayload(ByteBuffer.wrap(entry.getEncryptedPayload()))
                .setEncryptionKeyId(null)
                .setRPit(dataShare.getRPit())
                .setVersionConfiguration(null)
                .setDeviceNonce(null)
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
  }

  private static Map<String, Value> getSamplePayload(String uuid, long timestampSeconds) {
    Map<String, Value> samplePayload = new HashMap<>();

    Map<String, Value> prioParams = new HashMap<>();
    prioParams.put(
        DataShare.PRIME_FIELD, Value.newBuilder().setIntegerValue(DataShare.PRIME).build());
    prioParams.put(DataShare.BINS, Value.newBuilder().setIntegerValue(DEFAULT_BINS).build());
    prioParams.put(DataShare.EPSILON, Value.newBuilder().setDoubleValue(DEFAULT_EPSILON).build());
    prioParams.put(
        DataShare.NUMBER_OF_SERVERS_FIELD,
        Value.newBuilder().setIntegerValue(DataShare.NUMBER_OF_SERVERS).build());
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
    List<PrioDataSharePacket> phaShares = getSharesInFolder(phaDir, TEST_COLLECTION_NAME);
    Assert.assertTrue("No shares found in phaFolder", phaShares.size() > 0);
    Map<String, PrioDataSharePacket> phaUuidToPacket = new HashMap<>();
    for (PrioDataSharePacket packet : phaShares) {
      phaUuidToPacket.put(packet.getUuid().toString(), packet);
    }

    List<PrioDataSharePacket> facShares = getSharesInFolder(facDir, TEST_COLLECTION_NAME);
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
    }
  }

  private void verifyBatchOutput(String batchFolder)
      throws IOException, IllegalAccessException, InstantiationException,
          java.security.NoSuchAlgorithmException {
    Stream<Path> paths = Files.walk(Paths.get(batchFolder));
    List<Path> pathList = paths.filter(Files::isRegularFile).collect(Collectors.toList());
    int verifiedBatchesCount = 0;
    for (Path path : pathList) {
      if (!path.toString().endsWith(BatchWriterFn.DATASHARE_PACKET_SUFFIX)) {
        continue;
      }
      // Step 1: Verify the path format.
      String invalidPathMessage =
          "Expected file path to end in: {2-letter state"
              + " abbreviation}/{metricName}/YYYY/mm/dd/HH/MM/{batchId}{.batch, .batch.sig,"
              + " or .batch.avro}\n"
              + "Actual path: "
              + path.toString();
      Assert.assertTrue(
          invalidPathMessage,
          path.toString()
              .matches(
                  ".*/[a-z0-9_\\-]+(/\\d{4}/)(\\d{2}/){4}[a-zA-Z0-9\\-]+\\.batch($|.sig$|.avro$)"));
      String batchUuid =
          path.getFileName().toString().replace(BatchWriterFn.DATASHARE_PACKET_SUFFIX, "");
      Assert.assertEquals(
          "Invalid length of batchId.\n" + invalidPathMessage, 36, batchUuid.length());

      // Only consider shares created during this test run.
      PrioDataSharePacket packet =
          PrioSerializationHelper.deserializeRecords(PrioDataSharePacket.class, path.toString())
              .get(0);
      if (!packet.getUuid().toString().startsWith(TEST_COLLECTION_NAME)) {
        continue;
      }

      // Step 2: Verify the header file associated with this data share batch.
      byte[] packetBatchBytes = Files.readAllBytes(path);
      MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
      byte[] packetBatchHashDigest = sha256.digest(packetBatchBytes);

      PrioIngestionHeader expectedHeader =
          PrioSerializationHelper.createHeader(
              DataShareMetadata.builder()
                  .setBins(DEFAULT_BINS.intValue())
                  .setEpsilon(DEFAULT_EPSILON)
                  .setPrime(DataShare.PRIME)
                  .setNumberOfServers(DataShare.NUMBER_OF_SERVERS)
                  .setHammingWeight(DEFAULT_HAMMING_WEIGHT.intValue())
                  .setMetricName("fakeMetricName")
                  .build(),
              packetBatchHashDigest,
              UUID.fromString(batchUuid),
              CREATION_TIME,
              DURATION);
      String expectedHeaderFilename =
          path.getParent().toString() + "/" + batchUuid + BatchWriterFn.INGESTION_HEADER_SUFFIX;
      Assert.assertTrue(
          "Missing header file associated with batch id " + batchUuid,
          new File(expectedHeaderFilename).isFile());
      PrioIngestionHeader actualHeader =
          PrioSerializationHelper.deserializeRecords(
                  PrioIngestionHeader.class, expectedHeaderFilename)
              .get(0);
      Assert.assertEquals(expectedHeader.getBins(), actualHeader.getBins());
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
          "Missing signature file associated with batch id " + batchUuid,
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

  private List<PrioDataSharePacket> getSharesInFolder(String folder, String testCollectionUuid)
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
    // Don't include shares not generated in this test run.
    allDataSharesInFolder.removeIf(ds -> !ds.getUuid().toString().startsWith(testCollectionUuid));
    return allDataSharesInFolder;
  }

  private void compareSeededPacketsToActual(
      Map<String, List<PrioDataSharePacket>> inputDataSharePackets, String phaDir, String facDir)
      throws IllegalAccessException, IOException, InstantiationException {
    Map<String, List<PrioDataSharePacket>> actualDataSharePackets =
        readOutputShares(phaDir, facDir);
    for (Map.Entry<String, List<PrioDataSharePacket>> entry : actualDataSharePackets.entrySet()) {
      Assert.assertTrue(
          "Output contains data which is not present in input",
          inputDataSharePackets.containsKey(entry.getKey()));
      comparePrioDataSharePacket(
          entry.getValue().get(0), inputDataSharePackets.get(entry.getKey()).get(0));
      comparePrioDataSharePacket(
          entry.getValue().get(1), inputDataSharePackets.get(entry.getKey()).get(1));
    }
  }

  private List<String> getTestObjKeys(AmazonS3 s3Client, String bucketName, String testIdentifier) {
    List<S3ObjectSummary> objects = s3Client.listObjectsV2(bucketName).getObjectSummaries();
    List<DeleteObjectsRequest.KeyVersion> keysToDelete = new ArrayList<>();
    List<String> filteredKeys = new ArrayList<>();
    for (S3ObjectSummary os : objects) {
      String key = os.getKey();
      if (key.startsWith(testIdentifier)) {
        filteredKeys.add(key);
        keysToDelete.add(new DeleteObjectsRequest.KeyVersion(key));
      }
    }
    DeleteObjectsRequest multiObjectDeleteRequest =
        new DeleteObjectsRequest(testOptions.getPhaAwsBucketName())
            .withKeys(keysToDelete)
            .withQuiet(false);
    s3Client.deleteObjects(multiObjectDeleteRequest);
    return filteredKeys;
  }
}
