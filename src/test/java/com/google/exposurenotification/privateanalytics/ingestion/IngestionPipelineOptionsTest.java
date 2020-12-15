package com.google.exposurenotification.privateanalytics.ingestion;

import static com.google.common.truth.Truth.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link IngestionPipelineOptions}. */
@RunWith(JUnit4.class)
public class IngestionPipelineOptionsTest {

  @Test
  public void test_calculatePipelineStart() {
    assertThat(IngestionPipelineOptions.calculatePipelineStart(123, 5, Clock.systemUTC()))
        .isEqualTo(123);
    assertThat(
            IngestionPipelineOptions.calculatePipelineStart(
                IngestionPipelineOptions.UNSPECIFIED,
                10,
                Clock.fixed(Instant.ofEpochSecond(32), ZoneId.systemDefault())))
        .isEqualTo(20);
    assertThat(
            IngestionPipelineOptions.calculatePipelineStart(
                IngestionPipelineOptions.UNSPECIFIED,
                10,
                Clock.fixed(Instant.ofEpochSecond(20), ZoneId.systemDefault())))
        .isEqualTo(10);
    assertThat(
            IngestionPipelineOptions.calculatePipelineStart(
                IngestionPipelineOptions.UNSPECIFIED,
                // default ingestion pipeline window
                // https://github.com/google/exposure-notifications-private-analytics-ingestion/blob/ebf484edf5969d2b7113534db7450f61a937ecf0/terraform/variables.tf#L79
                3600,
                Clock.fixed(Instant.ofEpochSecond(1608067718), ZoneId.of("UTC"))))
        .isEqualTo(1608062400);
    assertThat(
            IngestionPipelineOptions.calculatePipelineStart(
                IngestionPipelineOptions.UNSPECIFIED,
                // default deletion pipeline window
                // https://github.com/google/exposure-notifications-private-analytics-ingestion/blob/ebf484edf5969d2b7113534db7450f61a937ecf0/terraform/variables.tf#L91
                43200,
                Clock.fixed(Instant.ofEpochSecond(1608033600), ZoneId.of("UTC"))))
        .isEqualTo(1607990400);
  }
}
