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
package com.google.exposurenotification.privateanalytics.ingestion.pipeline;

import com.google.exposurenotification.privateanalytics.ingestion.model.DataShare;
import com.google.exposurenotification.privateanalytics.ingestion.model.DataShare.DataShareMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.Values;
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
    List<String> certs = new ArrayList<>();
    certs.add("cert1");
    certs.add("cert2");
    certs.add("cert3");
    List<DataShare> inputData =
        Arrays.asList(
            DataShare.builder()
                .setCertificateChain(certs)
                .setPath("id1")
                .setCreatedMs(1000L)
                .setDataShareMetadata(meta)
                .build(),
            DataShare.builder()
                .setCertificateChain(certs)
                .setPath("id2")
                .setCreatedMs(2000L)
                .setDataShareMetadata(meta)
                .build(),
            DataShare.builder()
                .setCertificateChain(certs)
                .setPath("id3")
                .setCreatedMs(4000L)
                .setDataShareMetadata(meta)
                .build(),
            DataShare.builder()
                .setCertificateChain(certs)
                .setPath("missing")
                .setDataShareMetadata(meta)
                .build());

    PCollection<KV<DataShareMetadata, Iterable<DataShare>>> actualOutput =
        IngestionPipeline.processDataShares(pipeline.apply(Create.of(inputData)));

    List<Iterable<DataShare>> expectedValues =
        Arrays.asList(
            Collections.singletonList(
                DataShare.builder()
                    .setPath("id1")
                    .setCreatedMs(1000L)
                    .setCertificateChain(certs)
                    .setDataShareMetadata(meta)
                    .build()),
            Collections.singletonList(
                DataShare.builder()
                    .setPath("id2")
                    .setCreatedMs(2000L)
                    .setCertificateChain(certs)
                    .setDataShareMetadata(meta)
                    .build()));
    PAssert.that(actualOutput.apply(Keys.create()).apply(Count.globally())).containsInAnyOrder(2L);
    PAssert.that(actualOutput.apply(Values.create())).containsInAnyOrder(expectedValues);
    pipeline.run().waitUntilFinish();
  }
}
