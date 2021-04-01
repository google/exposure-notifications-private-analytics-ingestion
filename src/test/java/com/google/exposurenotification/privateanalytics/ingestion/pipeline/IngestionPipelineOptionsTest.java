/*
 * Copyright 2021 Google LLC
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
package com.google.exposurenotification.privateanalytics.ingestion.pipeline;

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
  public void testCalculatePipelineStart() {
    assertThat(IngestionPipelineOptions.calculatePipelineStart(123, 5, 1, Clock.systemUTC()))
        .isEqualTo(123);
    assertThat(IngestionPipelineOptions.calculatePipelineStart(123, 5, 4, Clock.systemUTC()))
        .isEqualTo(123);
    assertThat(
            IngestionPipelineOptions.calculatePipelineStart(
                IngestionPipelineOptions.UNSPECIFIED_START,
                10,
                1,
                Clock.fixed(Instant.ofEpochSecond(32), ZoneId.systemDefault())))
        .isEqualTo(20);
    assertThat(
            IngestionPipelineOptions.calculatePipelineStart(
                IngestionPipelineOptions.UNSPECIFIED_START,
                10,
                2,
                Clock.fixed(Instant.ofEpochSecond(32), ZoneId.systemDefault())))
        .isEqualTo(10);
    assertThat(
            IngestionPipelineOptions.calculatePipelineStart(
                IngestionPipelineOptions.UNSPECIFIED_START,
                10,
                1,
                Clock.fixed(Instant.ofEpochSecond(20), ZoneId.systemDefault())))
        .isEqualTo(10);
    assertThat(
            IngestionPipelineOptions.calculatePipelineStart(
                IngestionPipelineOptions.UNSPECIFIED_START,
                // default ingestion pipeline window
                // https://github.com/google/exposure-notifications-private-analytics-ingestion/blob/ebf484edf5969d2b7113534db7450f61a937ecf0/terraform/variables.tf#L79
                3600,
                1,
                Clock.fixed(Instant.ofEpochSecond(1608067718), ZoneId.of("UTC"))))
        .isEqualTo(1608062400);
    assertThat(
            IngestionPipelineOptions.calculatePipelineStart(
                IngestionPipelineOptions.UNSPECIFIED_START,
                // default deletion pipeline window
                // https://github.com/google/exposure-notifications-private-analytics-ingestion/blob/ebf484edf5969d2b7113534db7450f61a937ecf0/terraform/variables.tf#L91
                43200,
                2,
                Clock.fixed(Instant.ofEpochSecond(1608033600), ZoneId.of("UTC"))))
        .isEqualTo(1607947200);
  }
}
