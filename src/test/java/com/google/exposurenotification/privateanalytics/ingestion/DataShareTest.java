package com.google.exposurenotification.privateanalytics.ingestion;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.common.truth.Truth;
import java.util.HashMap;
import java.util.Map;
import org.junit.rules.ExpectedException;
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

  @Rule
  public ExpectedException thrown = ExpectedException.none();

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

    Truth.assertThat(dataShare.getId()).isEqualTo("id");
    Truth.assertThat(dataShare.getUuid()).isEqualTo("uniqueuserid");
    Truth.assertThat(dataShare.getRPit()).isEqualTo(7654321L);
    Truth.assertThat(dataShare.getPrime()).isEqualTo(4293918721L);
    Truth.assertThat(dataShare.getBins()).isEqualTo(2L);
    Truth.assertThat(dataShare.getHammingWeight()).isEqualTo(1L);
    Truth.assertThat(dataShare.getEpsilon()).isEqualTo(5.2933D);
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

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing required field: 'prioParams'");
    DataShare dataShare = DataShare.from(documentSnapshot);

    Truth.assertThat(dataShare.getId()).isEqualTo("id");
    Truth.assertThat(dataShare.getUuid()).isEqualTo("uniqueuserid");
    Truth.assertThat(dataShare.getRPit()).isEqualTo(7654321L);
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

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing required field: 'prime' from 'prioParams'");
    DataShare dataShare = DataShare.from(documentSnapshot);
  }

  @Test
  public void testWrongTypes() {
    when(documentSnapshot.getId()).thenReturn("id");
    when(documentSnapshot.get(eq(DataShare.CREATED))).thenReturn(3.14);
    thrown.expect(IllegalArgumentException.class);
    DataShare dataShare = DataShare.from(documentSnapshot);

    Truth.assertThat(dataShare.getId()).isEqualTo("id");
    Truth.assertThat(dataShare.getCreated()).isNull();
  }

}
