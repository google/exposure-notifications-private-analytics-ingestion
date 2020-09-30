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

import com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline.DateFilterFn;
import com.google.exposurenotification.privateanalytics.ingestion.IngestionPipeline.ForkByIndexFn;
import com.google.exposurenotification.privateanalytics.ingestion.SerializationFunctions.SerializeIngestionHeaderFn;
import com.google.exposurenotification.privateanalytics.ingestion.SerializationFunctions.SerializeDataShareFn;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.abetterinternet.prio.v1.PrioIngestionHeader;
import java.lang.AssertionError;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
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
public class SerializationFunctionsTest {

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category(ValidatesRunner.class)
    public void testSerializeDataShares() {
        IngestionPipelineOptions options = TestPipeline
                .testingPipelineOptions().as(IngestionPipelineOptions.class);
        options.setNumberOfServers(StaticValueProvider.of(2));
        List<Map<String, String>> sampleEncryptedDataShares = new ArrayList<>();
        Map<String, String> sampleDataShare1 = new HashMap<>();
        sampleDataShare1.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId1");
        sampleDataShare1.put(DataShare.PAYLOAD, "fakePayload1");
        Map<String, String> sampleDataShare2 = new HashMap<>();
        sampleDataShare2.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId2");
        sampleDataShare2.put(DataShare.PAYLOAD, "fakePayload2");
        sampleEncryptedDataShares.add(sampleDataShare1);
        sampleEncryptedDataShares.add(sampleDataShare2);
        List<DataShare> dataShares = Arrays.asList(
                DataShare.builder().setPath("id1").setCreated(1L).setRPit(12345L).setUuid("SuperUniqueId").setEpsilon(3.14D)
                        .setPrime(600613L).setBins(10).setNumberOfServers(2).setHammingWeight(10)
                        .setEncryptedDataShares(sampleEncryptedDataShares)
                        .build()
        );

        List<PrioDataSharePacket> avroDataShares = Arrays.asList(
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
                        .build()
        );
        PCollection<DataShare> input = pipeline.apply(Create.of(dataShares));
        PCollection<List<PrioDataSharePacket>> output =
                input.apply("SerializeDataShares", ParDo.of(new SerializeDataShareFn(options.getNumberOfServers())));

        PAssert.that(output).containsInAnyOrder(avroDataShares);
        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testSerializeHeaderFn() {
        IngestionPipelineOptions options = TestPipeline
                .testingPipelineOptions().as(IngestionPipelineOptions.class);
        options.setStartTime(StaticValueProvider.of(1L));
        options.setDuration(StaticValueProvider.of(2L));

        List<Map<String, String>> sampleEncryptedDataShares = new ArrayList<>();
        Map<String, String> sampleDataShare1 = new HashMap<>();
        sampleDataShare1.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId1");
        sampleDataShare1.put(DataShare.PAYLOAD, "fakePayload1");
        Map<String, String> sampleDataShare2 = new HashMap<>();
        sampleDataShare2.put(DataShare.ENCRYPTION_KEY_ID, "fakeEncryptionKeyId2");
        sampleDataShare2.put(DataShare.PAYLOAD, "fakePayload2");
        sampleEncryptedDataShares.add(sampleDataShare1);
        sampleEncryptedDataShares.add(sampleDataShare2);
        List<DataShare> dataShares = Arrays.asList(
                DataShare.builder().setPath("id1").setCreated(1L).setRPit(12345L).setUuid("SuperUniqueId").setEpsilon(3.14D)
                        .setPrime(600613L).setBins(10).setNumberOfServers(2).setHammingWeight(15)
                        .setEncryptedDataShares(sampleEncryptedDataShares)
                        .build()
        );

        PrioIngestionHeader expectedHeader =
                PrioIngestionHeader.newBuilder()
                        .setBatchUuid("placeholderUuid")
                        .setName("BatchUuid=placeholderUuid")
                        .setBatchStartTime(1L)
                        .setBatchEndTime(3L)
                        .setNumberOfServers(2)
                        .setBins(10)
                        .setHammingWeight(15)
                        .setPrime(600613L)
                        .setEpsilon(3.14D)
                        .setPacketFileDigest(ByteBuffer.wrap("placeholder".getBytes()))
                        .build();
        PCollection<DataShare> input = pipeline.apply(Create.of(dataShares));
        PCollection<PrioIngestionHeader> output =
                input.apply("SerializeIngestionHeaders", ParDo.of(
                        new SerializeIngestionHeaderFn(
                                options.getStartTime(),
                                options.getDuration())
                ));
        PAssert.that(output).containsInAnyOrder(expectedHeader);
        pipeline.run().waitUntilFinish();
    }
}