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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/**
 * Unit tests for {@link DataShare}.
 */
@RunWith(JUnit4.class)
public class DataShareTest {

  @Mock
  DocumentSnapshot documentSnapshot;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void testHappyCase() {
    Map<String, Object> samplePrioParams = new HashMap<>();
    samplePrioParams.put(DataShare.PRIME, 4293918721L);
    samplePrioParams.put(DataShare.BINS, 2L);
    samplePrioParams.put(DataShare.EPSILON, 5.2933D);
    samplePrioParams.put(DataShare.NUMBER_OF_SERVERS, 2L);
    samplePrioParams.put(DataShare.HAMMING_WEIGHT, 1L);

    List<Map<String, String>> sampleEncryptedDataShares = new ArrayList<>();
    Map<String, String> sampleDataShare1 = new HashMap<>();
    sampleDataShare1.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId1");
    sampleDataShare1.put(DataShare.PAYLOAD, "fakePayload1");
    Map<String, String> sampleDataShare2 = new HashMap<>();
    sampleDataShare2.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId2");
    sampleDataShare2.put(DataShare.PAYLOAD, "fakePayload2");
    sampleEncryptedDataShares.add(sampleDataShare1);
    sampleEncryptedDataShares.add(sampleDataShare2);

    Map<String, Object> samplePayload = new HashMap<>();
    samplePayload.put(DataShare.CREATED, Timestamp.ofTimeSecondsAndNanos(1234, 0));
    samplePayload.put(DataShare.UUID, "uniqueuserid");
    samplePayload.put(DataShare.ENCRYPTED_DATA_SHARES, sampleEncryptedDataShares);
    samplePayload.put(DataShare.PRIO_PARAMS, samplePrioParams);
    when(documentSnapshot.getId()).thenReturn("id");
    when(documentSnapshot.get(eq(DataShare.PAYLOAD)))
            .thenReturn(samplePayload);
    DataShare dataShare = DataShare.from(documentSnapshot);

    assertThat(dataShare.getId()).isEqualTo("id");
    assertThat(dataShare.getUuid()).isEqualTo("uniqueuserid");
    assertTrue(dataShare.getRPit() > 0L && dataShare.getRPit() < dataShare.getPrime());
    assertThat(dataShare.getPrime()).isEqualTo(4293918721L);
    assertThat(dataShare.getBins()).isEqualTo(2L);
    assertThat(dataShare.getHammingWeight()).isEqualTo(1L);
    assertThat(dataShare.getEpsilon()).isEqualTo(5.2933D);
    assertThat(dataShare.getEpsilon()).isEqualTo(5.2933D);
    assertThat(dataShare.getEncryptedDataShares()).isEqualTo(sampleEncryptedDataShares);
  }

  @Test
  public void testMissingPrioParams() {
    when(documentSnapshot.getId()).thenReturn("id");

    List<Map<String, String>> sampleEncryptedDataShares = new ArrayList<>();
    Map<String, String> sampleDataShare1 = new HashMap<>();
    sampleDataShare1.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId1");
    sampleDataShare1.put(DataShare.PAYLOAD, "fakePayload1");
    Map<String, String> sampleDataShare2 = new HashMap<>();
    sampleDataShare2.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId2");
    sampleDataShare2.put(DataShare.PAYLOAD, "fakePayload2");
    sampleEncryptedDataShares.add(sampleDataShare1);
    sampleEncryptedDataShares.add(sampleDataShare2);

    Map<String, Object> samplePayload = new HashMap<>();
    samplePayload.put(DataShare.CREATED, Timestamp.ofTimeSecondsAndNanos(1234, 0));
    samplePayload.put(DataShare.UUID, "uniqueuserid");
    samplePayload.put(DataShare.ENCRYPTED_DATA_SHARES, sampleEncryptedDataShares);
    samplePayload.remove(DataShare.PRIO_PARAMS);
    when(documentSnapshot.get(eq(DataShare.PAYLOAD)))
            .thenReturn(samplePayload);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> DataShare.from(documentSnapshot));
    assertThat(e).hasMessageThat().contains(
            "Missing required field: '" + DataShare.PRIO_PARAMS + "' from '" + DataShare.PAYLOAD + "'");
  }

  @Test
  public void testMissingPrime() {
    when(documentSnapshot.getId()).thenReturn("id");

    Map<String, Object> samplePrioParamsWithoutPrime = new HashMap<>();
    samplePrioParamsWithoutPrime.put(DataShare.BINS, 2L);
    samplePrioParamsWithoutPrime.put(DataShare.EPSILON, 5.2933D);
    samplePrioParamsWithoutPrime.put(DataShare.NUMBER_OF_SERVERS, 2L);
    samplePrioParamsWithoutPrime.put(DataShare.HAMMING_WEIGHT, 1L);

    List<Map<String, String>> sampleEncryptedDataShares = new ArrayList<>();
    Map<String, String> sampleDataShare1 = new HashMap<>();
    sampleDataShare1.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId1");
    sampleDataShare1.put(DataShare.PAYLOAD, "fakePayload1");
    Map<String, String> sampleDataShare2 = new HashMap<>();
    sampleDataShare2.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId2");
    sampleDataShare2.put(DataShare.PAYLOAD, "fakePayload2");
    sampleEncryptedDataShares.add(sampleDataShare1);
    sampleEncryptedDataShares.add(sampleDataShare2);

    Map<String, Object> samplePayload = new HashMap<>();
    samplePayload.put(DataShare.CREATED, Timestamp.ofTimeSecondsAndNanos(1234, 0));
    samplePayload.put(DataShare.UUID, "uniqueuserid");
    samplePayload.put(DataShare.ENCRYPTED_DATA_SHARES, sampleEncryptedDataShares);
    samplePayload.put(DataShare.PRIO_PARAMS, samplePrioParamsWithoutPrime);
    when(documentSnapshot.getId()).thenReturn("id");
    when(documentSnapshot.get(eq(DataShare.PAYLOAD)))
            .thenReturn(samplePayload);
    when(documentSnapshot.get(eq(DataShare.PAYLOAD)))
            .thenReturn(samplePayload);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> DataShare.from(documentSnapshot));
    assertThat(e).hasMessageThat().contains(
            "Missing required field: '" + DataShare.PRIME + "' from '" + DataShare.PRIO_PARAMS + "'");

  }

  @Test
  public void testWrongTypes() {
    Map<String, Object> samplePrioParams = new HashMap<>();
    samplePrioParams.put(DataShare.PRIME, 4293918721L);
    samplePrioParams.put(DataShare.BINS, 2L);
    samplePrioParams.put(DataShare.EPSILON, 5.2933D);
    samplePrioParams.put(DataShare.NUMBER_OF_SERVERS, 2L);
    samplePrioParams.put(DataShare.HAMMING_WEIGHT, 1L);

    List<Map<String, String>> sampleEncryptedDataShares = new ArrayList<>();
    Map<String, String> sampleDataShare1 = new HashMap<>();
    sampleDataShare1.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId1");
    sampleDataShare1.put(DataShare.PAYLOAD, "fakePayload1");
    Map<String, String> sampleDataShare2 = new HashMap<>();
    sampleDataShare2.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId2");
    sampleDataShare2.put(DataShare.PAYLOAD, "fakePayload2");
    sampleEncryptedDataShares.add(sampleDataShare1);
    sampleEncryptedDataShares.add(sampleDataShare2);

    Map<String, Object> samplePayload = new HashMap<>();
    samplePayload.put(DataShare.UUID, "uniqueuserid");
    samplePayload.put(DataShare.ENCRYPTED_DATA_SHARES, sampleEncryptedDataShares);
    samplePayload.put(DataShare.PRIO_PARAMS, samplePrioParams);
    samplePayload.put(DataShare.CREATED, 3.14);

    when(documentSnapshot.getId()).thenReturn("id");
    when(documentSnapshot.get(eq(DataShare.PAYLOAD)))
            .thenReturn(samplePayload);
    IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> DataShare.from(documentSnapshot));
    assertEquals(
  "Error casting '" + DataShare.CREATED + "' from '" + DataShare.PAYLOAD + "' to " + Timestamp.class.getName(),
            e.getMessage());
  }

}
