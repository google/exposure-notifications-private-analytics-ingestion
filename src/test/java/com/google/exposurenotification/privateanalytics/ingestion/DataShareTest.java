package com.google.exposurenotification.privateanalytics.ingestion;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.common.truth.Truth;
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
    when(documentSnapshot.get(eq(DataShare.CREATED)))
        .thenReturn(Timestamp.ofTimeSecondsAndNanos(1234, 0));

    DataShare dataShare = DataShare.from(documentSnapshot);

    Truth.assertThat(dataShare.getId()).isEqualTo("id");
    Truth.assertThat(dataShare.getCreated()).isEqualTo(1234L);
  }

  @Test
  public void testMissingFields() {
    when(documentSnapshot.getId()).thenReturn("id");

    DataShare dataShare = DataShare.from(documentSnapshot);

    Truth.assertThat(dataShare.getId()).isEqualTo("id");
  }

  @Test
  public void testWrongTypes() {
    when(documentSnapshot.getId()).thenReturn("id");
    when(documentSnapshot.get(eq(DataShare.CREATED))).thenReturn(3.14);

    DataShare dataShare = DataShare.from(documentSnapshot);

    Truth.assertThat(dataShare.getId()).isEqualTo("id");
    Truth.assertThat(dataShare.getCreated()).isNull();
  }

}
