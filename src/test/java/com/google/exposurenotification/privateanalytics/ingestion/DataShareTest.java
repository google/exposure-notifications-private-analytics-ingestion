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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Value.ValueTypeCase;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.DataShareMetadata;
import java.io.ByteArrayInputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link DataShare}.
 */
@RunWith(JUnit4.class)
public class DataShareTest {
  private static final Logger LOG = LoggerFactory.getLogger(DataShare.class);
  public static final String PATH_ID = "uuid/path/id";
  public static final String UUID = "uniqueuserid";
  public static final long PRIME = 4293918721L;
  public static final String SIGNATURE = "signature";
  public static final long BINS = 2L;
  public static final long HAMMING_WEIGHT = 1L;
  public static final double EPSILON = 5.2933D;

  Document document;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void testHappyCase() {
    Document.Builder docBuilder = Document.newBuilder();
    // Construct the payload.
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(prioParams, encryptedDataShares);
    Map<String, Value> fields = new HashMap<>();
    AbstractMap.SimpleEntry<List<X509Certificate>, List<Value>> certChains = createCertificateChain();
    List<X509Certificate> certs = certChains.getKey();
    List<Value> certsSerialized = certChains.getValue();
    fields.put(DataShare.CERT_CHAIN, Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(certsSerialized).build()).build());
    fields.put(DataShare.SIGNATURE, Value.newBuilder().setStringValue(SIGNATURE).build());
    fields.put(DataShare.PAYLOAD, Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build()).build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    DataShare dataShare = DataShare.from(document);
    DataShareMetadata metadata = dataShare.getDataShareMetadata();

    assertThat(dataShare.getPath()).isEqualTo(PATH_ID);
    assertThat(dataShare.getUuid()).isEqualTo(UUID);
    assertTrue(dataShare.getRPit() >= 0L && dataShare.getRPit() < metadata.getPrime());
    assertThat(metadata.getPrime()).isEqualTo(PRIME);
    assertThat(metadata.getBins()).isEqualTo(BINS);
    assertThat(metadata.getHammingWeight()).isEqualTo(HAMMING_WEIGHT);
    assertThat(metadata.getEpsilon()).isEqualTo(EPSILON);
    assertThat(dataShare.getEncryptedDataShares()).hasSize(2);
    assertThat(dataShare.getEncryptedDataShares().get(0).getEncryptionKeyId())
        .isEqualTo("fakeEncryptionKeyId1");
    assertThat(dataShare.getEncryptedDataShares().get(1).getEncryptionKeyId())
        .isEqualTo("fakeEncryptionKeyId2");
    assertThat(dataShare.getEncryptedDataShares().get(0).getEncryptedPayload())
        .isEqualTo("fakePayload1".getBytes());
    assertThat(dataShare.getEncryptedDataShares().get(1).getEncryptedPayload())
        .isEqualTo("fakePayload2".getBytes());
    assertThat(dataShare.getCertificateChain()).isEqualTo(certs);
    assertThat(dataShare.getSignature()).isEqualTo(SIGNATURE);
  }

  /** Tests with missing fields */

  @Test
  public void testMissingPrioParams() {
    Document.Builder docBuilder = Document.newBuilder();
    // Construct the payload.
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(prioParams, encryptedDataShares);
    // Remove the Prio params
    samplePayload.remove(DataShare.PRIO_PARAMS);Map<String, Value> fields = new HashMap<>();
    AbstractMap.SimpleEntry<List<X509Certificate>, List<Value>> certChains = createCertificateChain();
    List<Value> certsSerialized = certChains.getValue();
    fields.put(DataShare.CERT_CHAIN, Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(certsSerialized).build()).build());
    fields.put(DataShare.SIGNATURE, Value.newBuilder().setStringValue(SIGNATURE).build());
    fields.put(DataShare.PAYLOAD, Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build()).build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(document));

    assertThat(e).hasMessageThat()
        .contains("Missing required field: '" + DataShare.PRIO_PARAMS + "' from '" + DataShare.PAYLOAD + "'");
  }

  @Test
  public void testMissingPayload() {
    Document.Builder docBuilder = Document.newBuilder();
    Map<String, Value> fields = new HashMap<>();
    AbstractMap.SimpleEntry<List<X509Certificate>, List<Value>> certChains = createCertificateChain();
    List<Value> certsSerialized = certChains.getValue();
    fields.put(DataShare.CERT_CHAIN, Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(certsSerialized).build()).build());
    fields.put(DataShare.SIGNATURE, Value.newBuilder().setStringValue(SIGNATURE).build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(document));

    assertThat(e).hasMessageThat().contains("Missing required field: " + DataShare.PAYLOAD);
  }

  @Test
  public void testMissingSignature() {
    Document.Builder docBuilder = Document.newBuilder();
    // Construct the payload.
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(prioParams, encryptedDataShares);
    Map<String, Value> fields = new HashMap<>();
    AbstractMap.SimpleEntry<List<X509Certificate>, List<Value>> certChains = createCertificateChain();
    List<Value> certsSerialized = certChains.getValue();
    fields.put(DataShare.CERT_CHAIN, Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(certsSerialized).build()).build());
    fields.put(DataShare.PAYLOAD, Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build()).build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(document));

    assertThat(e).hasMessageThat().contains("Missing required field: '" + DataShare.SIGNATURE);
  }

  @Test
  public void testMissingCertChain() {
    Document.Builder docBuilder = Document.newBuilder();
    // Construct the payload.
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(prioParams, encryptedDataShares);
    Map<String, Value> fields = new HashMap<>();
    fields.put(DataShare.PAYLOAD, Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build()).build());
    fields.put(DataShare.SIGNATURE, Value.newBuilder().setStringValue(SIGNATURE).build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(document));

    assertThat(e).hasMessageThat().contains("Missing required field: " + DataShare.CERT_CHAIN);
  }

  @Test
  public void testMissingPrime() {
    Document.Builder docBuilder = Document.newBuilder();
    Map<String, Value> prioParams = createPrioParams();
    // Remove prime from Prio params
    prioParams.remove(DataShare.PRIME);
    // Construct payload
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(prioParams, encryptedDataShares);
    Map<String, Value> fields = new HashMap<>();
    AbstractMap.SimpleEntry<List<X509Certificate>, List<Value>> certChains = createCertificateChain();
    List<Value> certsSerialized = certChains.getValue();
    fields.put(DataShare.CERT_CHAIN, Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(certsSerialized).build()).build());
    fields.put(DataShare.SIGNATURE, Value.newBuilder().setStringValue(SIGNATURE).build());
    fields.put(DataShare.PAYLOAD, Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build()).build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(document));

    assertThat(e).hasMessageThat()
        .contains("Missing required field: '" + DataShare.PRIME + "' from '" + DataShare.PRIO_PARAMS + "'");
  }

  /** Test with incorrect values. */

  @Test
  public void testWrongTypes() {
    Document.Builder docBuilder = Document.newBuilder();
    // Construct the payload
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(prioParams, encryptedDataShares);
    // Set a payload field, CREATED, to invalid type
    samplePayload.replace(DataShare.CREATED, Value.newBuilder().setStringValue("false").build());
    Map<String, Value> fields = new HashMap<>();
    AbstractMap.SimpleEntry<List<X509Certificate>, List<Value>> certChains = createCertificateChain();
    List<Value> certsSerialized = certChains.getValue();
    fields.put(DataShare.CERT_CHAIN, Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(certsSerialized).build()).build());
    fields.put(DataShare.SIGNATURE, Value.newBuilder().setStringValue(SIGNATURE).build());
    fields.put(DataShare.PAYLOAD, Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build()).build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(document));

    assertEquals(
        "Error casting '" + DataShare.CREATED + "' from '" + DataShare.PAYLOAD + "' to " + ValueTypeCase.TIMESTAMP_VALUE.name(),
        e.getMessage());
  }

  @Test
  public void testIncorrectCertificates() {
    Document.Builder docBuilder = Document.newBuilder();
    // Construct payload
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(prioParams, encryptedDataShares);
    Map<String, Value> fields = new HashMap<>();
    List<Value> certsSerialized = new ArrayList<>();
    certsSerialized.add(Value.newBuilder().setStringValue("Incorrect serialization").build());
    fields.put(DataShare.CERT_CHAIN, Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(certsSerialized).build()).build());
    fields.put(DataShare.SIGNATURE, Value.newBuilder().setStringValue(SIGNATURE).build());
    fields.put(DataShare.PAYLOAD, Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build()).build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(document));

    assertThat(e).hasMessageThat().contains("Could not parse the chain of certificates: " + DataShare.CERT_CHAIN);
  }

  /** Static functions to create the objects used in the tests above. */
  public static Map<String, Value> createPrioParams() {
    Map<String, Value> samplePrioParams = new HashMap<>();
    samplePrioParams.put(DataShare.PRIME, Value.newBuilder().setIntegerValue(PRIME).build());
    samplePrioParams.put(DataShare.BINS, Value.newBuilder().setIntegerValue(BINS).build());
    samplePrioParams.put(DataShare.EPSILON, Value.newBuilder().setDoubleValue(EPSILON).build());
    samplePrioParams.put(DataShare.NUMBER_OF_SERVERS_FIELD, Value.newBuilder().setIntegerValue(BINS).build());
    samplePrioParams.put(DataShare.HAMMING_WEIGHT, Value.newBuilder().setIntegerValue(
        HAMMING_WEIGHT).build());
    return samplePrioParams;
  }

  public static List<Value> createEncryptedDataShares() {
    List<Value> sampleEncryptedDataShares = new ArrayList<>();
    Map<String, Value> sampleDataShare1 = new HashMap<>();
    sampleDataShare1.put(DataShare.ENCRYPTION_KEY_ID, Value.newBuilder().setStringValue("fakeEncryptionKeyId1").build());
    sampleDataShare1.put(DataShare.PAYLOAD, Value.newBuilder().setStringValue(Base64.getEncoder().encodeToString("fakePayload1".getBytes())).build());
    Map<String, Value> sampleDataShare2 = new HashMap<>();
    sampleDataShare2.put(DataShare.ENCRYPTION_KEY_ID, Value.newBuilder().setStringValue("fakeEncryptionKeyId2").build());
    sampleDataShare2.put(DataShare.PAYLOAD, Value.newBuilder().setStringValue(Base64.getEncoder().encodeToString("fakePayload2".getBytes())).build());
    sampleEncryptedDataShares.add(Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(sampleDataShare1).build()).build());
    sampleEncryptedDataShares.add(Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(sampleDataShare2).build()).build());
    return sampleEncryptedDataShares;
  }

  public static Map<String, Value> createPayload(Map<String, Value> prioParams,
      List<Value> encryptedDataShares) {
    Map<String, Value> samplePayload = new HashMap<>();
    samplePayload.put(DataShare.CREATED, Value.newBuilder().setTimestampValue(
        com.google.protobuf.Timestamp.newBuilder().setSeconds(1234).build()).build());
    samplePayload.put(DataShare.UUID, Value.newBuilder().setStringValue(UUID).build());
    samplePayload.put(DataShare.ENCRYPTED_DATA_SHARES, Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(encryptedDataShares)).build());
    samplePayload.put(DataShare.PRIO_PARAMS, Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(prioParams).build()).build());
    return samplePayload;
  }

  public static AbstractMap.SimpleEntry<List<X509Certificate>, List<Value>> createCertificateChain() {
    List<X509Certificate> certificates = new ArrayList<>();
    List<Value> certsSerialized = new ArrayList<>();
    try {
      String certBase64 = getTestCertificate();
      byte[] certBytes = Base64.getDecoder().decode(certBase64);
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      X509Certificate cert = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(certBytes));
      LOG.info("parsed: " + cert.toString());
      certificates.add(cert);
      certificates.add(cert); // twice
      certsSerialized.add(Value.newBuilder().setStringValue(certBase64).build());
      certsSerialized.add(Value.newBuilder().setStringValue(certBase64).build());
    } catch (Exception e) {
      // pass: it's a CertificateException in case we mistyped "X.509".
    }
    return new AbstractMap.SimpleEntry<>(certificates, certsSerialized);
  }

  public static String getTestCertificate() {
    // Valid certificate as exported by our client code.
    return "MIICyDCCAm2gAwIBAgIBATAMBggqhkjOPQQDAgUAMC8xGTAXBgNVBAUTEDkwZThkYTNjYWRmYzc4MjAxEjAQBgNVBAwMCVN0cm9uZ0JveDAeFw0xMzA3MjUxMjU2NTRaFw0xNjAzMzAxNjU2NDJaMB8xHTAbBgNVBAMMFEFuZHJvaWQgS2V5c3RvcmUgS2V5MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAES3oA8SxQRQfTnBDdMFTERIC38T1y5DQdtVqosqjB3V/o05aczU20EDulfobidcr1N4UspphJhEF9QKIbb18YtKOCAYYwggGCMA4GA1UdDwEB/wQEAwIHgDCCAW4GCisGAQQB1nkCAREEggFeMIIBWgIBBAoBAgIBKQoBAgQg6hlB6Biw0IWHMrxnsmalvx6wFd5e6N7hGlrjhsq1KJ0EADCBhb+DEQgCBgF02q9YSr+DEggCBgF02q9YSr+FPQgCBgF01aRzlr+FRV0EWzBZMTMwMQQsY29tLmdvb2dsZS5hbmRyb2lkLmFwcHMuZXhwb3N1cmVub3RpZmljYXRpb24CAQExIgQgN3X23FDoH2zp6mldxSrVqY4oBntFBDYM4HdzkJUFZH8wgZ+hCDEGAgECAgEDogMCAQOjBAICAQClBTEDAgEEv4N3AgUAv4U+AwIBAL+FQEwwSgQgrmMWtHU8YfWFW5W5uYSEr3hPLoNkjQ/MgQf8p1LK6jQBAf8KAQAEIFvj6AqMmElSJ8eBKUhqgsb8USJq/9FGNDsO7PW0mIw5v4VBBQIDAa2wv4VCBQIDAxUav4VOBgIEATQ+Lb+FTwYCBAE0Pi0wDAYIKoZIzj0EAwIFAANHADBEAiAjXwY3sMOoce6mWycLSssC0kjrMXSURzwTIl6gSQmK5AIgYUFCdKpJDfoZEkLwcT+k3lXDXbLGXwjywsAxYOZhgX4=";
  }
}
