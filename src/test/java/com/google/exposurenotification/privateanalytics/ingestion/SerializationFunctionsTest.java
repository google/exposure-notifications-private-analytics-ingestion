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

import com.google.exposurenotification.privateanalytics.ingestion.DataShare.DataShareMetadata;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.EncryptedShare;
import com.google.exposurenotification.privateanalytics.ingestion.SerializationFunctions.SerializeDataShareFn;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
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
        List<EncryptedShare> sampleEncryptedDataShares = new ArrayList<>();
        sampleEncryptedDataShares.add(EncryptedShare.builder()
            .setEncryptionKeyId("fakeEncryptionKeyId1")
            .setEncryptedPayload("fakePayload1".getBytes())
            .build());
        sampleEncryptedDataShares.add(EncryptedShare.builder()
            .setEncryptionKeyId("fakeEncryptionKeyId2")
            .setEncryptedPayload("fakePayload2".getBytes())
            .build());
        DataShareMetadata metadata = DataShareMetadata.builder()
            .setEpsilon(3.14D)
            .setPrime(600613L)
            .setBins(10)
            .setNumberOfServers(2)
            .setHammingWeight(10)
            .build();
        List<DataShare> dataShares = Arrays.asList(
            DataShare.builder()
                .setPath("id1")
                .setCreated(1L)
                .setRPit(12345L)
                .setUuid("SuperUniqueId")
                .setDataShareMetadata(
                    metadata)
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
        PCollection<KV<DataShareMetadata, List<PrioDataSharePacket>>> output =
            input.apply("SerializeDataShares",
                ParDo.of(new SerializeDataShareFn("dummyMetric")));

        PAssert.that(output).containsInAnyOrder(KV.of(metadata, avroDataShares));
        pipeline.run().waitUntilFinish();
    }
}