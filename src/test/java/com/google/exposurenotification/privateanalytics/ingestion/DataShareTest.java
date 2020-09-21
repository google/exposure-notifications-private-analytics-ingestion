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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
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
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/**
 * Unit tests for {@link DataShare}.
 */
@RunWith(JUnit4.class)
public class DataShareTest {

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  DocumentSnapshot documentSnapshot;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void testHappyCase() {

    // Construct the payload.
    Map<String, Object> prioParams = createPrioParams();
    List<Map<String, String>> encryptedDataShares = createEncryptedDataShares();
    Map<String, Object> samplePayload = createPayload(prioParams, encryptedDataShares);

    // Signature and chain of certificates.
    String signature = "signature";
    AbstractMap.SimpleEntry<List<X509Certificate>, List<String>> certChains = createCertificateChain();
    List<X509Certificate> certs = certChains.getKey();
    List<String> certsSerialized = certChains.getValue();

    // Specify the documentSnapshot
    when(documentSnapshot.getReference().getPath()).thenReturn("/path/id");
    when(documentSnapshot.get(eq(DataShare.PAYLOAD))).thenReturn(samplePayload);
    when(documentSnapshot.get(eq(DataShare.SIGNATURE))).thenReturn(signature);
    when(documentSnapshot.get(eq(DataShare.CERT_CHAIN))).thenReturn(certsSerialized);

    DataShare dataShare = DataShare.from(documentSnapshot);

    assertThat(dataShare.getPath()).isEqualTo("/path/id");
    assertThat(dataShare.getUuid()).isEqualTo("uniqueuserid");
    assertTrue(dataShare.getRPit() >= 0L && dataShare.getRPit() < dataShare.getPrime());
    assertThat(dataShare.getPrime()).isEqualTo(4293918721L);
    assertThat(dataShare.getBins()).isEqualTo(2L);
    assertThat(dataShare.getHammingWeight()).isEqualTo(1L);
    assertThat(dataShare.getEpsilon()).isEqualTo(5.2933D);
    assertThat(dataShare.getEncryptedDataShares()).isEqualTo(encryptedDataShares);
    assertThat(dataShare.getCertificateChain()).isEqualTo(certs);
    assertThat(dataShare.getSignature()).isEqualTo(signature);
  }

  /** Tests with missing fields */

  @Test
  public void testMissingPrioParams() {
    // Construct the payload.
    Map<String, Object> prioParams = createPrioParams();
    List<Map<String, String>> encryptedDataShares = createEncryptedDataShares();
    Map<String, Object> samplePayload = createPayload(prioParams, encryptedDataShares);

    // Signature and chain of certificates.
    String signature = "signature";
    AbstractMap.SimpleEntry<List<X509Certificate>, List<String>> certChains = createCertificateChain();
    List<String> certsSerialized = certChains.getValue();

    // Remove the Prio params
    samplePayload.remove(DataShare.PRIO_PARAMS);

    // Specify the documentSnapshot
    when(documentSnapshot.getReference().getPath()).thenReturn("/path/id");
    when(documentSnapshot.get(eq(DataShare.PAYLOAD))).thenReturn(samplePayload);
    when(documentSnapshot.get(eq(DataShare.SIGNATURE))).thenReturn(signature);
    when(documentSnapshot.get(eq(DataShare.CERT_CHAIN))).thenReturn(certsSerialized);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(documentSnapshot));
    assertThat(e).hasMessageThat()
        .contains("Missing required field: '" + DataShare.PRIO_PARAMS + "' from '" + DataShare.PAYLOAD + "'");
  }

  @Test
  public void testMissingPayload() {
    // Signature and chain of certificates.
    String signature = "signature";
    AbstractMap.SimpleEntry<List<X509Certificate>, List<String>> certChains = createCertificateChain();
    List<String> certsSerialized = certChains.getValue();

    // Specify the documentSnapshot
    when(documentSnapshot.getReference().getPath()).thenReturn("/path/id");
    when(documentSnapshot.get(eq(DataShare.PAYLOAD))).thenReturn(null);
    when(documentSnapshot.get(eq(DataShare.SIGNATURE))).thenReturn(signature);
    when(documentSnapshot.get(eq(DataShare.CERT_CHAIN))).thenReturn(certsSerialized);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(documentSnapshot));
    assertThat(e).hasMessageThat()
        .contains("Missing required field: " + DataShare.PAYLOAD);
  }

  @Test
  public void testMissingSignature() {
     // Construct the payload.
     Map<String, Object> prioParams = createPrioParams();
     List<Map<String, String>> encryptedDataShares = createEncryptedDataShares();
     Map<String, Object> samplePayload = createPayload(prioParams, encryptedDataShares);

    // Chain of certificates.
    AbstractMap.SimpleEntry<List<X509Certificate>, List<String>> certChains = createCertificateChain();
    List<String> certsSerialized = certChains.getValue();

    // Specify the documentSnapshot
    when(documentSnapshot.getReference().getPath()).thenReturn("/path/id");
    when(documentSnapshot.get(eq(DataShare.PAYLOAD))).thenReturn(samplePayload);
    when(documentSnapshot.get(eq(DataShare.SIGNATURE))).thenReturn(null);
    when(documentSnapshot.get(eq(DataShare.CERT_CHAIN))).thenReturn(certsSerialized);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(documentSnapshot));
    assertThat(e).hasMessageThat()
        .contains("Missing required field: " + DataShare.SIGNATURE);
  }

  @Test
  public void testMissingCertChain() {
     // Construct the payload.
     Map<String, Object> prioParams = createPrioParams();
     List<Map<String, String>> encryptedDataShares = createEncryptedDataShares();
     Map<String, Object> samplePayload = createPayload(prioParams, encryptedDataShares);

    // Signature
    String signature = "signature";

    // Specify the documentSnapshot
    when(documentSnapshot.getReference().getPath()).thenReturn("/path/id");
    when(documentSnapshot.get(eq(DataShare.PAYLOAD))).thenReturn(samplePayload);
    when(documentSnapshot.get(eq(DataShare.SIGNATURE))).thenReturn(signature);
    when(documentSnapshot.get(eq(DataShare.CERT_CHAIN))).thenReturn(null);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(documentSnapshot));
    assertThat(e).hasMessageThat()
        .contains("Missing required field: " + DataShare.CERT_CHAIN);
  }

  @Test
  public void testMissingPrime() {
    // Construct the payload.
    Map<String, Object> prioParamsWithoutPrime = createPrioParams();
    List<Map<String, String>> encryptedDataShares = createEncryptedDataShares();

    // Signature and chain of certificates.
    String signature = "signature";
    AbstractMap.SimpleEntry<List<X509Certificate>, List<String>> certChains = createCertificateChain();
    List<String> certsSerialized = certChains.getValue();

    // Remove the prime
    prioParamsWithoutPrime.remove(DataShare.PRIME);
    Map<String, Object> samplePayload = createPayload(prioParamsWithoutPrime, encryptedDataShares);

    // Specify the documentSnapshot
    when(documentSnapshot.getReference().getPath()).thenReturn("/path/id");
    when(documentSnapshot.get(eq(DataShare.PAYLOAD))).thenReturn(samplePayload);
    when(documentSnapshot.get(eq(DataShare.SIGNATURE))).thenReturn(signature);
    when(documentSnapshot.get(eq(DataShare.CERT_CHAIN))).thenReturn(certsSerialized);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(documentSnapshot));
    assertThat(e).hasMessageThat()
        .contains("Missing required field: '" + DataShare.PRIME + "' from '" + DataShare.PRIO_PARAMS + "'");
  }

  /** Test with incorrect values. */

  @Test
  public void testWrongTypes() {
    when(documentSnapshot.getReference().getPath()).thenReturn("/path/id");

    // Construct the payload.
    Map<String, Object> prioParams = createPrioParams();
    List<Map<String, String>> encryptedDataShares = createEncryptedDataShares();
    Map<String, Object> samplePayload = createPayload(prioParams, encryptedDataShares);

    // Signature and chain of certificates.
    String signature = "signature";
    AbstractMap.SimpleEntry<List<X509Certificate>, List<String>> certChains = createCertificateChain();
    List<String> certsSerialized = certChains.getValue();

    // Modify the payload
    samplePayload.replace(DataShare.CREATED, 3.14);

    // Specify the documentSnapshot
    when(documentSnapshot.getReference().getPath()).thenReturn("/path/id");
    when(documentSnapshot.get(eq(DataShare.PAYLOAD))).thenReturn(samplePayload);
    when(documentSnapshot.get(eq(DataShare.SIGNATURE))).thenReturn(signature);
    when(documentSnapshot.get(eq(DataShare.CERT_CHAIN))).thenReturn(certsSerialized);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(documentSnapshot));
    assertEquals(
        "Error casting '" + DataShare.CREATED + "' from '" + DataShare.PAYLOAD + "' to " + Timestamp.class.getName(),
        e.getMessage());
  }

  @Test
  public void testIncorrectCertificates() {
    // Construct the payload.
    Map<String, Object> prioParams = createPrioParams();
    List<Map<String, String>> encryptedDataShares = createEncryptedDataShares();
    Map<String, Object> samplePayload = createPayload(prioParams, encryptedDataShares);

    // Signature and chain of certificates.
    String signature = "signature";
    List<String> certsSerialized = new ArrayList<>();
    certsSerialized.add("Incorrect serialization");

    // Specify the documentSnapshot
    when(documentSnapshot.getReference().getPath()).thenReturn("/path/id");
    when(documentSnapshot.get(eq(DataShare.PAYLOAD))).thenReturn(samplePayload);
    when(documentSnapshot.get(eq(DataShare.SIGNATURE))).thenReturn(signature);
    when(documentSnapshot.get(eq(DataShare.CERT_CHAIN))).thenReturn(certsSerialized);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DataShare.from(documentSnapshot));
    assertThat(e).hasMessageThat()
        .contains("Could not parse the chain of certificates: " + DataShare.CERT_CHAIN );
  }

  /** Static functions to create the objects used in the tests above. */ 
  public static Map<String, Object> createPrioParams() {
    Map<String, Object> samplePrioParams = new HashMap<>();
    samplePrioParams.put(DataShare.PRIME, 4293918721L);
    samplePrioParams.put(DataShare.BINS, 2L);
    samplePrioParams.put(DataShare.EPSILON, 5.2933D);
    samplePrioParams.put(DataShare.NUMBER_OF_SERVERS, 2L);
    samplePrioParams.put(DataShare.HAMMING_WEIGHT, 1L);
    return samplePrioParams;
  }

  public static List<Map<String, String>> createEncryptedDataShares() {
    List<Map<String, String>> sampleEncryptedDataShares = new ArrayList<>();
    Map<String, String> sampleDataShare1 = new HashMap<>();
    sampleDataShare1.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId1");
    sampleDataShare1.put(DataShare.PAYLOAD, "fakePayload1");
    Map<String, String> sampleDataShare2 = new HashMap<>();
    sampleDataShare2.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId2");
    sampleDataShare2.put(DataShare.PAYLOAD, "fakePayload2");
    sampleEncryptedDataShares.add(sampleDataShare1);
    sampleEncryptedDataShares.add(sampleDataShare2);
    return sampleEncryptedDataShares;
  }

  public static Map<String, Object> createPayload(Map<String, Object> prioParams,
      List<Map<String, String>> encryptedDataShares) {
    Map<String, Object> samplePayload = new HashMap<>();
    samplePayload.put(DataShare.CREATED, Timestamp.ofTimeSecondsAndNanos(1234, 0));
    samplePayload.put(DataShare.UUID, "uniqueuserid");
    samplePayload.put(DataShare.ENCRYPTED_DATA_SHARES, encryptedDataShares);
    samplePayload.put(DataShare.PRIO_PARAMS, prioParams);
    return samplePayload;
  }

  public static AbstractMap.SimpleEntry<List<X509Certificate>, List<String>> createCertificateChain() {
    List<X509Certificate> certificates = new ArrayList<>();
    List<String> certsSerialized = new ArrayList<>();
    try {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      X509Certificate cert = (X509Certificate) cf.generateCertificate(getTestCertificate());
      certificates.add(cert);
      certificates.add(cert); // twice
      certsSerialized.add(Base64.getEncoder().encodeToString(cert.getEncoded()));
      certsSerialized.add(Base64.getEncoder().encodeToString(cert.getEncoded()));
    }
    catch(Exception e) {
      // pass: it's a CertificateException in case we mistyped "X.509".
    }
    return new AbstractMap.SimpleEntry<>(certificates, certsSerialized);
  }

  public static InputStream getTestCertificate() {
    // Valid X509 certificate generated using OpenSSL.
    final String cert = "-----BEGIN CERTIFICATE-----"
        + "MIIBsjCCAVegAwIBAgIUPXQ5XjNVNijqr4tJ/TL9NmRBCo8wCgYIKoZIzj0EAwIw"
        + "LjELMAkGA1UEBhMCVVMxEzARBgNVBAgMClNvbWUtU3RhdGUxCjAIBgNVBAoMASAw"
        + "HhcNMjAwOTE3MjMxNzAzWhcNMjEwOTE3MjMxNzAzWjAuMQswCQYDVQQGEwJVUzET"
        + "MBEGA1UECAwKU29tZS1TdGF0ZTEKMAgGA1UECgwBIDBZMBMGByqGSM49AgEGCCqG"
        + "SM49AwEHA0IABB2JVqnNEkLl5slL9fAN5n4gAiSYas7zJ9NopSkRXqrZ/VWIYbK+"
        + "sbGdK1YigR4xc0P2Fky1zqIara4aVPduFXujUzBRMB0GA1UdDgQWBBSFBpKi66Dr"
        + "1Mw5BqiruZehqWcx4jAfBgNVHSMEGDAWgBSFBpKi66Dr1Mw5BqiruZehqWcx4jAP"
        + "BgNVHRMBAf8EBTADAQH/MAoGCCqGSM49BAMCA0kAMEYCIQC52jryRk3uN/ML0POm"
        + "aiCFkohiPX6EEmP53kkpnQM8NgIhAOVumygGGJqdJdtKJc+RRnMUYKztpyYw/eKd" + "1xDFK4Le" + "-----END CERTIFICATE-----";
    return new ByteArrayInputStream(cert.getBytes());
  }
}
