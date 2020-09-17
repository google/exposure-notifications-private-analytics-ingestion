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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import java.util.HashMap;
import java.util.Map;
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
    when(documentSnapshot.getId()).thenReturn("id");
    when(documentSnapshot.getTimestamp(eq(DataShare.CREATED)))
        .thenReturn(Timestamp.ofTimeSecondsAndNanos(1234, 0));
    when(documentSnapshot.getString(eq(DataShare.UUID)))
            .thenReturn("uniqueuserid");
    when(documentSnapshot.getLong(eq(DataShare.R_PIT)))
            .thenReturn(7654321L);

    Map<String, Object> mockPrioParams = new HashMap<>();
    mockPrioParams.put(DataShare.PRIME, 4293918721L);
    mockPrioParams.put(DataShare.BINS, 2L);
    mockPrioParams.put(DataShare.EPSILON, 5.2933D);
    mockPrioParams.put(DataShare.NUMBER_OF_SERVERS, 2L);
    mockPrioParams.put(DataShare.HAMMING_WEIGHT, 1L);
    when(documentSnapshot.get(eq(DataShare.PRIO_PARAMS)))
            .thenReturn(mockPrioParams);

    DataShare dataShare = DataShare.from(documentSnapshot);

    assertThat(dataShare.getId()).isEqualTo("id");
    assertThat(dataShare.getUuid()).isEqualTo("uniqueuserid");
    assertThat(dataShare.getRPit()).isEqualTo(7654321L);
    assertThat(dataShare.getPrime()).isEqualTo(4293918721L);
    assertThat(dataShare.getBins()).isEqualTo(2L);
    assertThat(dataShare.getHammingWeight()).isEqualTo(1L);
    assertThat(dataShare.getEpsilon()).isEqualTo(5.2933D);
  }

  @Test
  public void testMissingPrioParams() {
    when(documentSnapshot.getId()).thenReturn("id");
    when(documentSnapshot.getTimestamp(eq(DataShare.CREATED)))
            .thenReturn(Timestamp.ofTimeSecondsAndNanos(1234, 0));
    when(documentSnapshot.getString(eq(DataShare.UUID)))
            .thenReturn("uniqueuserid");
    when(documentSnapshot.getLong(eq(DataShare.R_PIT)))
            .thenReturn(7654321L);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> DataShare.from(documentSnapshot));
    assertThat(e).hasMessageThat().contains("Missing required field: 'prioParams'");
  }

  @Test
  public void testMissingPrime() {
    when(documentSnapshot.getId()).thenReturn("id");
    when(documentSnapshot.getTimestamp(eq(DataShare.CREATED)))
            .thenReturn(Timestamp.ofTimeSecondsAndNanos(1234, 0));
    when(documentSnapshot.getString(eq(DataShare.UUID)))
            .thenReturn("uniqueuserid");
    when(documentSnapshot.getLong(eq(DataShare.R_PIT)))
            .thenReturn(7654321L);

    Map<String, Object> mockPrioParams = new HashMap<>();
    mockPrioParams.put(DataShare.BINS, 2L);
    mockPrioParams.put(DataShare.EPSILON, 5.2933D);
    mockPrioParams.put(DataShare.NUMBER_OF_SERVERS, 2L);
    mockPrioParams.put(DataShare.HAMMING_WEIGHT, 1L);
    when(documentSnapshot.get(eq(DataShare.PRIO_PARAMS)))
            .thenReturn(mockPrioParams);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> DataShare.from(documentSnapshot));
    assertThat(e).hasMessageThat().contains("Missing required field: 'prime' from 'prioParams'");

  }

  @Test
  public void testWrongTypes() {
    when(documentSnapshot.getId()).thenReturn("id");
    when(documentSnapshot.get(eq(DataShare.CREATED))).thenReturn(3.14);
    assertThrows(IllegalArgumentException.class,
        () -> DataShare.from(documentSnapshot));
  }

}
