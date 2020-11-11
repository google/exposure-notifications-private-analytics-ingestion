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

import com.google.exposurenotification.privateanalytics.ingestion.DataShare.DataShareMetadata;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.InvalidDataShareException;
import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Value.ValueTypeCase;
import java.math.BigInteger;
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

/** Unit tests for {@link DataShare}. */
@RunWith(JUnit4.class)
public class DataShareTest {

  public static final String PATH_ID = "uuid/path/id";
  public static final String METRIC_NAME = "id";
  public static final String UUID = "uniqueuserid";
  public static final String SIGNATURE = "signature";
  public static final Integer BINS = 2;
  public static final Integer HAMMING_WEIGHT = 1;
  public static final double EPSILON = 5.2933D;
  public static final Integer SCHEMA_VERSION = 1;
  public static final String ENCRYPTION_KEY_1 = "fakeEncryptionKeyId1";
  public static final String ENCRYPTION_KEY_2 = "fakeEncryptionKeyId2";
  public static final String PAYLOAD_1 = "fakePayload1";
  public static final String PAYLOAD_2 = "fakePayload2";
  public static final Integer CREATED = 1234;

  Document document;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void testHappyCase() {
    DataShare dataShare = createFakeDataShare(CREATED, PATH_ID);

    DataShareMetadata metadata = dataShare.getDataShareMetadata();
    assertThat(metadata).isEqualTo(createDataShareMetadata());

    List<DataShare.EncryptedShare> encryptedShares = dataShare.getEncryptedDataShares();
    assertThat(encryptedShares).hasSize(DataShare.NUMBER_OF_SERVERS);
    assertThat(encryptedShares).isEqualTo(createListEncryptedShares());

    assertThat(dataShare.getPath()).isEqualTo(PATH_ID);
    assertThat(dataShare.getUuid()).isEqualTo(UUID);

    assertTrue(dataShare.getRPit() > 0L && dataShare.getRPit() < metadata.getPrime());
    // r_PIT cannot be equal to any of the n-th root of unity where n = next_power_two(#bins + 1).
    long n = DataShare.nextPowerTwo(metadata.getBins() + 1);
    BigInteger N = BigInteger.valueOf(n);
    BigInteger P = BigInteger.valueOf(metadata.getPrime());
    assertThat(BigInteger.valueOf(dataShare.getRPit()).modPow(N, P)).isNotEqualTo(BigInteger.ONE);

    assertThat(dataShare.getSignature())
        .isEqualTo(Base64.getEncoder().encodeToString(SIGNATURE.getBytes()));
  }

  /** Tests with missing fields */
  @Test
  public void testMissingPrioParams() {
    Document.Builder docBuilder = Document.newBuilder();
    // Construct the payload.
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(CREATED, prioParams, encryptedDataShares);
    // Remove the Prio params
    samplePayload.remove(DataShare.PRIO_PARAMS);
    Map<String, Value> fields = new HashMap<>();
    fields.put(
        DataShare.CERT_CHAIN,
        Value.newBuilder()
            .setArrayValue(
                ArrayValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("cert1").build())
                    .addValues(Value.newBuilder().setStringValue("cert2").build())
                    .build())
            .build());
    fields.put(
        DataShare.SIGNATURE,
        Value.newBuilder()
            .setStringValue(Base64.getEncoder().encodeToString(SIGNATURE.getBytes()))
            .build());
    fields.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build())
            .build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    InvalidDataShareException e =
        assertThrows(InvalidDataShareException.class, () -> DataShare.from(document));

    assertThat(e)
        .hasMessageThat()
        .contains(
            "Missing required field: '"
                + DataShare.PRIO_PARAMS
                + "' from '"
                + DataShare.PAYLOAD
                + "'");
  }

  @Test
  public void testMissingPayload() {
    Document.Builder docBuilder = Document.newBuilder();
    Map<String, Value> fields = new HashMap<>();
    fields.put(
        DataShare.CERT_CHAIN,
        Value.newBuilder()
            .setArrayValue(
                ArrayValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("cert1").build())
                    .addValues(Value.newBuilder().setStringValue("cert2").build())
                    .build())
            .build());
    fields.put(
        DataShare.SIGNATURE,
        Value.newBuilder()
            .setStringValue(Base64.getEncoder().encodeToString(SIGNATURE.getBytes()))
            .build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    InvalidDataShareException e =
        assertThrows(InvalidDataShareException.class, () -> DataShare.from(document));

    assertThat(e).hasMessageThat().contains("Missing required field: " + DataShare.PAYLOAD);
  }

  @Test
  public void testMissingSignature() {
    Document.Builder docBuilder = Document.newBuilder();
    // Construct the payload.
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(CREATED, prioParams, encryptedDataShares);
    Map<String, Value> fields = new HashMap<>();
    fields.put(
        DataShare.CERT_CHAIN,
        Value.newBuilder()
            .setArrayValue(
                ArrayValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("cert1").build())
                    .addValues(Value.newBuilder().setStringValue("cert2").build())
                    .build())
            .build());
    fields.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build())
            .build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    InvalidDataShareException e =
        assertThrows(InvalidDataShareException.class, () -> DataShare.from(document));

    assertThat(e).hasMessageThat().contains("Missing required field: '" + DataShare.SIGNATURE);
  }

  @Test
  public void testMissingCertChain() {
    Document.Builder docBuilder = Document.newBuilder();
    // Construct the payload.
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(CREATED, prioParams, encryptedDataShares);
    Map<String, Value> fields = new HashMap<>();
    fields.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build())
            .build());
    fields.put(
        DataShare.SIGNATURE,
        Value.newBuilder()
            .setStringValue(Base64.getEncoder().encodeToString(SIGNATURE.getBytes()))
            .build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    InvalidDataShareException e =
        assertThrows(InvalidDataShareException.class, () -> DataShare.from(document));

    assertThat(e).hasMessageThat().contains("Missing required field: " + DataShare.CERT_CHAIN);
  }

  @Test
  public void testMissingPrime() {
    Document.Builder docBuilder = Document.newBuilder();
    Map<String, Value> prioParams = createPrioParams();
    // Remove prime from Prio params
    prioParams.remove(DataShare.PRIME_FIELD);
    // Construct payload
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(CREATED, prioParams, encryptedDataShares);
    Map<String, Value> fields = new HashMap<>();
    fields.put(
        DataShare.CERT_CHAIN,
        Value.newBuilder()
            .setArrayValue(
                ArrayValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("cert1").build())
                    .addValues(Value.newBuilder().setStringValue("cert2").build())
                    .build())
            .build());
    fields.put(
        DataShare.SIGNATURE,
        Value.newBuilder()
            .setStringValue(Base64.getEncoder().encodeToString(SIGNATURE.getBytes()))
            .build());
    fields.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build())
            .build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    InvalidDataShareException e =
        assertThrows(InvalidDataShareException.class, () -> DataShare.from(document));

    assertThat(e)
        .hasMessageThat()
        .contains(
            "Missing required field: '"
                + DataShare.PRIME_FIELD
                + "' from '"
                + DataShare.PRIO_PARAMS
                + "'");
  }

  /** Test with incorrect values. */
  @Test
  public void testWrongTypes() {
    Document.Builder docBuilder = Document.newBuilder();
    // Construct the payload
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(CREATED, prioParams, encryptedDataShares);
    // Set a payload field, CREATED, to invalid type
    samplePayload.replace(DataShare.CREATED, Value.newBuilder().setStringValue("false").build());
    Map<String, Value> fields = new HashMap<>();
    fields.put(
        DataShare.CERT_CHAIN,
        Value.newBuilder()
            .setArrayValue(
                ArrayValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("cert1").build())
                    .addValues(Value.newBuilder().setStringValue("cert2").build())
                    .build())
            .build());
    fields.put(
        DataShare.SIGNATURE,
        Value.newBuilder()
            .setStringValue(Base64.getEncoder().encodeToString(SIGNATURE.getBytes()))
            .build());
    fields.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build())
            .build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    InvalidDataShareException e =
        assertThrows(InvalidDataShareException.class, () -> DataShare.from(document));

    assertEquals(
        "Error casting '"
            + DataShare.CREATED
            + "' from '"
            + DataShare.PAYLOAD
            + "' to "
            + ValueTypeCase.TIMESTAMP_VALUE.name(),
        e.getMessage());
  }

  /** Test with incorrect schema version. */
  @Test
  public void testWrongSchemaVersion() {
    Integer invalidSchemaVersion = DataShare.LATEST_SCHEMA_VERSION + 1;
    Document.Builder docBuilder = Document.newBuilder();
    // Construct the payload
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(CREATED, prioParams, encryptedDataShares);
    samplePayload.replace(
        DataShare.SCHEMA_VERSION,
        Value.newBuilder().setIntegerValue(DataShare.LATEST_SCHEMA_VERSION).build(),
        Value.newBuilder().setIntegerValue(invalidSchemaVersion).build());
    Map<String, Value> fields = new HashMap<>();
    fields.put(
        DataShare.CERT_CHAIN,
        Value.newBuilder()
            .setArrayValue(
                ArrayValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("cert1").build())
                    .addValues(Value.newBuilder().setStringValue("cert2").build())
                    .build())
            .build());
    fields.put(
        DataShare.SIGNATURE,
        Value.newBuilder()
            .setStringValue(Base64.getEncoder().encodeToString(SIGNATURE.getBytes()))
            .build());
    fields.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build())
            .build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    InvalidDataShareException e =
        assertThrows(InvalidDataShareException.class, () -> DataShare.from(document));

    assertThat(e).hasMessageThat().contains("Invalid schema version: " + invalidSchemaVersion);
  }

  /** Test with smaller schema version. */
  @Test
  public void testSmallerSchemaVersion() {
    Integer validSchemaVersion = DataShare.LATEST_SCHEMA_VERSION - 1; // will be >= 1
    Document.Builder docBuilder = Document.newBuilder();
    // Construct the payload
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(CREATED, prioParams, encryptedDataShares);
    samplePayload.replace(
        DataShare.SCHEMA_VERSION,
        Value.newBuilder().setIntegerValue(DataShare.LATEST_SCHEMA_VERSION).build(),
        Value.newBuilder().setIntegerValue(validSchemaVersion).build());
    Map<String, Value> fields = new HashMap<>();
    fields.put(
        DataShare.CERT_CHAIN,
        Value.newBuilder()
            .setArrayValue(
                ArrayValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("cert1").build())
                    .addValues(Value.newBuilder().setStringValue("cert2").build())
                    .build())
            .build());
    fields.put(
        DataShare.SIGNATURE,
        Value.newBuilder()
            .setStringValue(Base64.getEncoder().encodeToString(SIGNATURE.getBytes()))
            .build());
    fields.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build())
            .build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    DataShare.from(document);
  }

  /** Test with schema version = 0. */
  @Test
  public void testZeroSchemaVersion() {
    Integer zeroSchemaVersion = 0; // will be >= 1
    Document.Builder docBuilder = Document.newBuilder();
    // Construct the payload
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(CREATED, prioParams, encryptedDataShares);
    samplePayload.replace(
        DataShare.SCHEMA_VERSION,
        Value.newBuilder().setIntegerValue(DataShare.LATEST_SCHEMA_VERSION).build(),
        Value.newBuilder().setIntegerValue(zeroSchemaVersion).build());
    Map<String, Value> fields = new HashMap<>();
    fields.put(
        DataShare.CERT_CHAIN,
        Value.newBuilder()
            .setArrayValue(
                ArrayValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("cert1").build())
                    .addValues(Value.newBuilder().setStringValue("cert2").build())
                    .build())
            .build());
    fields.put(
        DataShare.SIGNATURE,
        Value.newBuilder()
            .setStringValue(Base64.getEncoder().encodeToString(SIGNATURE.getBytes()))
            .build());
    fields.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build())
            .build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    InvalidDataShareException e =
        assertThrows(InvalidDataShareException.class, () -> DataShare.from(document));

    assertThat(e).hasMessageThat().contains("Invalid schema version: " + zeroSchemaVersion);
  }

  /** Test with missing schema version. */
  @Test
  public void testMissingSchemaVersion() {
    Document.Builder docBuilder = Document.newBuilder();
    // Construct the payload
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(CREATED, prioParams, encryptedDataShares);
    samplePayload.remove(DataShare.SCHEMA_VERSION);
    Map<String, Value> fields = new HashMap<>();
    fields.put(
        DataShare.CERT_CHAIN,
        Value.newBuilder()
            .setArrayValue(
                ArrayValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("cert1").build())
                    .addValues(Value.newBuilder().setStringValue("cert2").build())
                    .build())
            .build());
    fields.put(
        DataShare.SIGNATURE,
        Value.newBuilder()
            .setStringValue(Base64.getEncoder().encodeToString(SIGNATURE.getBytes()))
            .build());
    fields.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build())
            .build());
    docBuilder.setName(PATH_ID);
    docBuilder.putAllFields(fields);
    document = docBuilder.build();

    InvalidDataShareException e =
        assertThrows(InvalidDataShareException.class, () -> DataShare.from(document));

    assertThat(e).hasMessageThat().contains("Missing required field: " + DataShare.SCHEMA_VERSION);
  }

  /** Test for NextPower2 function. */
  @Test
  public void testNextPowerTwo() {
    assertThat(DataShare.nextPowerTwo(0)).isEqualTo(1L);
    assertThat(DataShare.nextPowerTwo(1)).isEqualTo(1L);
    assertThat(DataShare.nextPowerTwo(2)).isEqualTo(2L);
    for (int i = 2; i < 31; i++) {
      assertThat(DataShare.nextPowerTwo(1 << i)).isEqualTo(1L << i);
      assertThat(DataShare.nextPowerTwo((1 << i) - 1)).isEqualTo(1L << i);
      assertThat(DataShare.nextPowerTwo((1 << i) + 1)).isEqualTo(1L << (i + 1));
    }

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> DataShare.nextPowerTwo(-1));
    assertThat(e).hasMessageThat().contains("cannot be < 0");
  }

  /** Static functions to create the objects used in the tests above. */
  public static DataShare createFakeDataShare(Integer timestamp, String path) {
    Document.Builder docBuilder = Document.newBuilder();
    Map<String, Value> prioParams = createPrioParams();
    List<Value> encryptedDataShares = createEncryptedDataShares();
    Map<String, Value> samplePayload = createPayload(timestamp, prioParams, encryptedDataShares);
    Map<String, Value> fields = new HashMap<>();
    fields.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(samplePayload).build())
            .build());
    fields.put(
        DataShare.SIGNATURE,
        Value.newBuilder()
            .setStringValue(Base64.getEncoder().encodeToString(SIGNATURE.getBytes()))
            .build());
    fields.put(
        DataShare.CERT_CHAIN,
        Value.newBuilder()
            .setArrayValue(
                ArrayValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("cert1").build())
                    .addValues(Value.newBuilder().setStringValue("cert2").build())
                    .build())
            .build());
    docBuilder.setName(path);
    docBuilder.putAllFields(fields);
    return DataShare.from(docBuilder.build());
  }

  public static Map<String, Value> createPrioParams() {
    Map<String, Value> samplePrioParams = new HashMap<>();
    samplePrioParams.put(
        DataShare.PRIME_FIELD, Value.newBuilder().setIntegerValue(DataShare.PRIME).build());
    samplePrioParams.put(DataShare.BINS, Value.newBuilder().setIntegerValue(BINS).build());
    samplePrioParams.put(DataShare.EPSILON, Value.newBuilder().setDoubleValue(EPSILON).build());
    samplePrioParams.put(
        DataShare.NUMBER_OF_SERVERS_FIELD,
        Value.newBuilder().setIntegerValue(DataShare.NUMBER_OF_SERVERS).build());
    samplePrioParams.put(
        DataShare.HAMMING_WEIGHT, Value.newBuilder().setIntegerValue(HAMMING_WEIGHT).build());
    return samplePrioParams;
  }

  public static List<Value> createEncryptedDataShares() {
    List<Value> sampleEncryptedDataShares = new ArrayList<>();
    Map<String, Value> sampleDataShare1 = new HashMap<>();
    sampleDataShare1.put(
        DataShare.ENCRYPTION_KEY_ID, Value.newBuilder().setStringValue(ENCRYPTION_KEY_1).build());
    sampleDataShare1.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setStringValue(Base64.getEncoder().encodeToString(PAYLOAD_1.getBytes()))
            .build());
    Map<String, Value> sampleDataShare2 = new HashMap<>();
    sampleDataShare2.put(
        DataShare.ENCRYPTION_KEY_ID, Value.newBuilder().setStringValue(ENCRYPTION_KEY_2).build());
    sampleDataShare2.put(
        DataShare.PAYLOAD,
        Value.newBuilder()
            .setStringValue(Base64.getEncoder().encodeToString(PAYLOAD_2.getBytes()))
            .build());
    sampleEncryptedDataShares.add(
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(sampleDataShare1).build())
            .build());
    sampleEncryptedDataShares.add(
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(sampleDataShare2).build())
            .build());
    return sampleEncryptedDataShares;
  }

  public static Map<String, Value> createPayload(
      Integer timestamp, Map<String, Value> prioParams, List<Value> encryptedDataShares) {
    Map<String, Value> samplePayload = new HashMap<>();
    samplePayload.put(
        DataShare.CREATED,
        Value.newBuilder()
            .setTimestampValue(
                com.google.protobuf.Timestamp.newBuilder().setSeconds(timestamp).build())
            .build());
    samplePayload.put(DataShare.UUID, Value.newBuilder().setStringValue(UUID).build());
    samplePayload.put(
        DataShare.ENCRYPTED_DATA_SHARES,
        Value.newBuilder()
            .setArrayValue(ArrayValue.newBuilder().addAllValues(encryptedDataShares))
            .build());
    samplePayload.put(
        DataShare.PRIO_PARAMS,
        Value.newBuilder()
            .setMapValue(MapValue.newBuilder().putAllFields(prioParams).build())
            .build());
    samplePayload.put(
        DataShare.SCHEMA_VERSION,
        Value.newBuilder().setIntegerValue(DataShare.LATEST_SCHEMA_VERSION).build());
    return samplePayload;
  }

  public static DataShareMetadata createDataShareMetadata() {
    return DataShareMetadata.builder()
        .setBins(BINS)
        .setEpsilon(EPSILON)
        .setHammingWeight(HAMMING_WEIGHT)
        .setMetricName(METRIC_NAME)
        .setNumberOfServers(DataShare.NUMBER_OF_SERVERS)
        .setPrime(DataShare.PRIME)
        .build();
  }

  public static List<DataShare.EncryptedShare> createListEncryptedShares() {
    List<DataShare.EncryptedShare> encryptedShares = new ArrayList<>();
    encryptedShares.add(
        DataShare.EncryptedShare.builder()
            .setEncryptionKeyId(ENCRYPTION_KEY_1)
            .setEncryptedPayload(PAYLOAD_1.getBytes())
            .build());
    encryptedShares.add(
        DataShare.EncryptedShare.builder()
            .setEncryptionKeyId(ENCRYPTION_KEY_2)
            .setEncryptedPayload(PAYLOAD_2.getBytes())
            .build());
    return encryptedShares;
  }
}
