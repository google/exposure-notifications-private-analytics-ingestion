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

import com.google.exposurenotification.privateanalytics.ingestion.DataShare.DataShareMetadata;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link IngestionPipeline}. */
@RunWith(JUnit4.class)
public class IngestionPipelineTest {

  public transient IngestionPipelineOptions options =
      TestPipeline.testingPipelineOptions().as(IngestionPipelineOptions.class);

  @Rule public final transient TestPipeline pipeline = TestPipeline.fromOptions(options);

  @Test
  @Category(ValidatesRunner.class)
  public void processDataShares_valid() {
    options.setStartTime(1L);
    options.setDuration(2L);
    options.setBatchSize(1L);
    options.setDeviceAttestation(false);

    DataShareMetadata meta = DataShareMetadata.builder().setMetricName("sampleMetric").build();
    List<DataShare> inputData =
        Arrays.asList(
            DataShare.builder().setPath("id1").setCreated(1L).setDataShareMetadata(meta).build(),
            DataShare.builder().setPath("id2").setCreated(2L).setDataShareMetadata(meta).build(),
            DataShare.builder().setPath("id3").setCreated(4L).setDataShareMetadata(meta).build(),
            DataShare.builder().setPath("missing").setDataShareMetadata(meta).build());

    PCollection<KV<DataShareMetadata, Iterable<DataShare>>> actualOutput =
        IngestionPipeline.processDataShares(pipeline.apply(Create.of(inputData)));

    List<KV<DataShareMetadata, Iterable<DataShare>>> expectedOutput =
        Arrays.asList(
            KV.of(
                meta,
                Collections.singletonList(
                    DataShare.builder()
                        .setPath("id1")
                        .setCreated(1L)
                        .setDataShareMetadata(meta)
                        .build())),
            KV.of(
                meta,
                Collections.singletonList(
                    DataShare.builder()
                        .setPath("id2")
                        .setCreated(2L)
                        .setDataShareMetadata(meta)
                        .build())));
    PAssert.that(actualOutput).containsInAnyOrder(expectedOutput);
    pipeline.run().waitUntilFinish();
  }

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
  }
}
