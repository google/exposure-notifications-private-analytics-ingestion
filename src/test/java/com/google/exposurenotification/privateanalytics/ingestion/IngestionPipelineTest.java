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

import com.google.exposurenotification.privateanalytics.ingestion.SerializationFunctions.ForkByIndexFn;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link IngestionPipeline}.
 */
@RunWith(JUnit4.class)
public class IngestionPipelineTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testDateFilter() {
    List<DataShare> dataShares = Arrays.asList(
        DataShare.builder().setPath("id1").setCreated(1L).build(),
        DataShare.builder().setPath("id2").setCreated(2L).build(),
        DataShare.builder().setPath("id3").setCreated(3L).build(),
        DataShare.builder().setPath("missing").build()
    );
    PCollection<DataShare> input = pipeline.apply(Create.of(dataShares));

    PCollection<DataShare> output =
            input.apply(
                    ParDo.of(new DateFilterFn(StaticValueProvider.of(2L), StaticValueProvider.of(1L))));

    PAssert.that(output).containsInAnyOrder(
        Collections.singletonList(
            DataShare.builder().setPath("id2").setCreated(2L)
                .build()));
    pipeline.run().waitUntilFinish();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void processDataShares_valid() {
    IngestionPipelineOptions options = TestPipeline
            .testingPipelineOptions().as(IngestionPipelineOptions.class);
    options.setStartTime(StaticValueProvider.of(2L));
    options.setDuration(StaticValueProvider.of(1L));
    options.setMinimumParticipantCount(StaticValueProvider.of(1L));
    List<DataShare> inputData = Arrays.asList(
        DataShare.builder().setPath("id1").setCreated(1L).build(),
        DataShare.builder().setPath("id2").setCreated(2L).build(),
        DataShare.builder().setPath("id3").setCreated(4L).build(),
        DataShare.builder().setPath("missing").build()
    );
    List<DataShare> expectedOutput =
        Arrays.asList(DataShare.builder().setPath("id2").setCreated(2L).build());

    PCollection<DataShare> actualOutput = IngestionPipeline
            .processDataShares(pipeline.apply(Create.of(inputData)), options);

    PAssert.that(actualOutput).containsInAnyOrder(expectedOutput);
    pipeline.run().waitUntilFinish();
  }

  @Test(expected = AssertionError.class)
  @Category(ValidatesRunner.class)
  public void processDataShares_participantCountlessThanMinCount() {
    IngestionPipelineOptions options = TestPipeline
            .testingPipelineOptions().as(IngestionPipelineOptions.class);
    options.setStartTime(StaticValueProvider.of(2L));
    options.setDuration(StaticValueProvider.of(1L));
    options.setMinimumParticipantCount(StaticValueProvider.of(2L));
    List<DataShare> inputData = Arrays.asList(
        DataShare.builder().setPath("id1").setCreated(1L).build(),
        DataShare.builder().setPath("id2").setCreated(2L).build(),
        DataShare.builder().setPath("id3").setCreated(4L).build(),
        DataShare.builder().setPath("missing").build()
    );

    IngestionPipeline
            .processDataShares(pipeline.apply(Create.of(inputData)), options);
    pipeline.run().waitUntilFinish();
  }


  @Test
  @Category(ValidatesRunner.class)
  public void testForkDataSharesFn() {
    IngestionPipelineOptions options = TestPipeline
        .testingPipelineOptions().as(IngestionPipelineOptions.class);

    List<List<PrioDataSharePacket>> dataSharesInput =
        Arrays.asList(
            Arrays.asList(
                PrioDataSharePacket.newBuilder()
                    .setEncryptionKeyId("fakeEncryptionKeyId1")
                    .setEncryptedPayload(ByteBuffer.wrap("fakePayload1".getBytes()))
                    .setRPit(12345L)
                    .setUuid("SuperUniqueId")
                    .build(),
                PrioDataSharePacket.newBuilder()
                    .setEncryptionKeyId("fakeEncryptionKeyId2")
                    .setEncryptedPayload(ByteBuffer.wrap("fakePayload2".getBytes()))
                    .setRPit(12345L)
                    .setUuid("SuperUniqueId")
                    .build()),

            Arrays.asList(
                PrioDataSharePacket.newBuilder()
                    .setEncryptionKeyId("bogusEncryptionKeyId1")
                    .setEncryptedPayload(ByteBuffer.wrap("bogusPayload1".getBytes()))
                    .setRPit(600613L)
                    .setUuid("AnotherUniqueId")
                    .build(),
                    PrioDataSharePacket.newBuilder()
                    .setEncryptionKeyId("bogusEncryptionKeyId2")
                    .setEncryptedPayload(ByteBuffer.wrap("bogusPayload2".getBytes()))
                    .setRPit(600613L)
                    .setUuid("AnotherUniqueId")
                    .build()));

    int index = 1;
    DataShareMetadata metadata = DataShareMetadata.builder().build();
    List<KV<DataShareMetadata, PrioDataSharePacket>> dataSharesInIndex = new ArrayList<>();
    for (List<PrioDataSharePacket> dataSharePackets : dataSharesInput) {
      dataSharesInIndex.add(KV.of(metadata, dataSharePackets.get(index)));
    }

    PCollection<KV<DataShareMetadata, PrioDataSharePacket>> output =
        pipeline.apply(Create.of(dataSharesInput))
            .apply(WithKeys.of(metadata))
            .apply("ForkDataShares", ParDo.of(new ForkByIndexFn(index)));

    PAssert.thatSingleton(output.apply("CountDataShares", Count.globally()))
        .satisfies(input -> {
          Assert.assertTrue(
              "Number of data shares returned does not match number of data shares in index "
                  + index
                  + " from dataSharesInput",
              input == dataSharesInIndex.size());
              return null;
            });

    PAssert.that(output).containsInAnyOrder(dataSharesInIndex);
    pipeline.run().waitUntilFinish();
  }

}