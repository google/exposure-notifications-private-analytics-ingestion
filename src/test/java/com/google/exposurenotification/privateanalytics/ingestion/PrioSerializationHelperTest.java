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
import static org.junit.Assert.assertEquals;

import com.google.exposurenotification.privateanalytics.ingestion.DataShare.EncryptedShare;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.abetterinternet.prio.v1.PrioIngestionHeader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrioSerializationHelper}. */
@RunWith(JUnit4.class)
public class PrioSerializationHelperTest {

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testPrioBatchHeaderSerialization()
      throws IOException, InstantiationException, IllegalAccessException {
    List<PrioIngestionHeader> ingestionHeaders = new ArrayList<>();
    PrioIngestionHeader header1 =
        PrioIngestionHeader.newBuilder()
            .setBatchUuid("id123")
            .setName("secretname")
            .setBins(123)
            .setEpsilon(3.14)
            .setPrime(7)
            .setNumberOfServers(3)
            .setHammingWeight(5)
            .setBatchStartTime(1600000000)
            .setBatchEndTime(1700000000)
            .setPacketFileDigest(ByteBuffer.wrap("placeholder1".getBytes()))
            .build();
    PrioIngestionHeader header2 =
        PrioIngestionHeader.newBuilder()
            .setBatchUuid("id987")
            .setName("simplename")
            .setBins(4)
            .setEpsilon(2.71)
            .setPrime(13)
            .setNumberOfServers(5)
            .setHammingWeight(8)
            .setBatchStartTime(1650000000)
            .setBatchEndTime(1710000000)
            .setPacketFileDigest(ByteBuffer.wrap("placeholder2".getBytes()))
            .build();
    ingestionHeaders.add(header1);
    ingestionHeaders.add(header2);
    File serializedHeaders = tmpFolder.newFile();
    ByteBuffer resultBytes =
        PrioSerializationHelper.serializeRecords(
            ingestionHeaders, PrioIngestionHeader.class, PrioIngestionHeader.getClassSchema());

    BatchWriterFn.writeToFile(serializedHeaders.getAbsolutePath(), resultBytes);
    List<PrioIngestionHeader> deserializedHeaders =
        PrioSerializationHelper.deserializeRecords(
            PrioIngestionHeader.class, serializedHeaders.getAbsolutePath());
    assertEquals(ingestionHeaders, deserializedHeaders);
  }

  @Test
  public void testPrioDataSharePacketSerialization()
      throws IOException, InstantiationException, IllegalAccessException {
    List<PrioDataSharePacket> dataSharePackets = new ArrayList<>();
    PrioDataSharePacket dataSharePacket1 =
        PrioDataSharePacket.newBuilder()
            .setDeviceNonce(ByteBuffer.wrap(new byte[] {0x07, 0x08, 0x09}))
            .setEncryptionKeyId("verysecretandsecurevalue1")
            .setRPit(1234567890)
            .setUuid("uniqueuserid1")
            .setVersionConfiguration("v1.0")
            .setEncryptedPayload(ByteBuffer.wrap(new byte[] {0x01, 0x02, 0x03, 0x04, 0x05}))
            .build();

    PrioDataSharePacket dataSharePacket2 =
        PrioDataSharePacket.newBuilder()
            .setDeviceNonce(ByteBuffer.wrap(new byte[] {0x10, 0x11, 0x12}))
            .setEncryptionKeyId("verysecretandsecurevalue2")
            .setRPit(987654321)
            .setUuid("uniqueuserid2")
            .setVersionConfiguration("v2.0")
            .setEncryptedPayload(ByteBuffer.wrap(new byte[] {0x06, 0x07, 0x08, 0x09, 0x10}))
            .build();
    dataSharePackets.add(dataSharePacket1);
    dataSharePackets.add(dataSharePacket2);

    File serializedDataShares = tmpFolder.newFile();
    ByteBuffer resultBytes =
        PrioSerializationHelper.serializeRecords(
            dataSharePackets, PrioDataSharePacket.class, PrioDataSharePacket.getClassSchema());
    BatchWriterFn.writeToFile(serializedDataShares.getAbsolutePath(), resultBytes);
    List<PrioDataSharePacket> deserializedHeaders =
        PrioSerializationHelper.deserializeRecords(
            PrioDataSharePacket.class, serializedDataShares.getAbsolutePath());
    assertEquals(dataSharePackets, deserializedHeaders);
  }

  @Test
  public void testSplitPackets() {
    DataShare share =
        DataShare.builder()
            .setSchemaVersion(2)
            .setEncryptedDataShares(
                List.of(
                    EncryptedShare.builder()
                        .setEncryptedPayload("pha".getBytes())
                        .setEncryptionKeyId("55NdHuhCjyR3PtTL0A7WRiaIgURhTmlkNw5dbFsKL70=")
                        .build(),
                    EncryptedShare.builder()
                        .setEncryptedPayload("facilitator".getBytes())
                        .setEncryptionKeyId("facilitator-key-id")
                        .build()))
            .setRPit(2L)
            .setUuid("someuuid")
            .build();
    URL manifestUrl =
        getClass()
            .getResource(
                "/java/com/google/exposurenotification/privateanalytics/ingestion/test-manifest.json");
    DataProcessorManifest phaManifest = new DataProcessorManifest(manifestUrl.toString());

    List<PrioDataSharePacket> packets = PrioSerializationHelper.splitPackets(share);
    assertThat(packets).hasSize(2);
    assertThat(packets.get(0).getEncryptionKeyId()).isNull();
    assertThat(packets.get(1).getEncryptionKeyId()).isNull();
  }
}
