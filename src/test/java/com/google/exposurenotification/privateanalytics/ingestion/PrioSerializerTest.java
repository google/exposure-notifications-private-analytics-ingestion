package com.google.exposurenotification.privateanalytics.ingestion;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrioSerializer}. */
@RunWith(JUnit4.class)
public class PrioSerializerTest {

    @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void testPrioBatchHeaderSerialization() throws IOException {
        List<ENPA.PrioBatchHeader> batchHeaders = new ArrayList<>();
        ENPA.PrioBatchHeader header1 =
                ENPA.PrioBatchHeader.newBuilder()
                        .setBatchUuid("id123")
                        .setName("secretname")
                        .setBins(123)
                        .setEpsilon(3.14)
                        .setPrime(7)
                        .setNumberOfServers(3)
                        .setHammingWeight(5)
                        .setBatchStartTime(1600000000)
                        .setBatchEndTime(1700000000)
                        .setCertificateHash("hashyhash")
                        .setSignatureOfPackets(ByteBuffer.wrap(new byte[] {0x10, 0x20, 0x30}))
                        .build();
        ENPA.PrioBatchHeader header2 =
                ENPA.PrioBatchHeader.newBuilder()
                        .setBatchUuid("id987")
                        .setName("simplename")
                        .setBins(4)
                        .setEpsilon(2.71)
                        .setPrime(13)
                        .setNumberOfServers(5)
                        .setHammingWeight(8)
                        .setBatchStartTime(1650000000)
                        .setBatchEndTime(1710000000)
                        .setCertificateHash("hashedpotatoes")
                        .setSignatureOfPackets(ByteBuffer.wrap(new byte[] {0x12, 0x13, 0x14}))
                        .build();
        batchHeaders.add(header1);
        batchHeaders.add(header2);

        File serializedHeaders = tmpFolder.newFile();
        PrioSerializer.serializeBatchHeaders(batchHeaders, serializedHeaders.getAbsolutePath());
        List<ENPA.PrioBatchHeader> deserializedHeaders =
                PrioSerializer.deserializeBatchHeaders(serializedHeaders.getAbsolutePath());
        assertEquals(batchHeaders, deserializedHeaders);
    }

    @Test
    public void testPrioBatchHeaderSignatureSerialization() throws IOException {
        List<ENPA.PrioBatchHeaderSignature> signatures = new ArrayList<>();
        ENPA.PrioBatchHeaderSignature signature1 =
                ENPA.PrioBatchHeaderSignature.newBuilder()
                        .setBatchHeaderSignature(ByteBuffer.wrap(new byte[] {0x01, 0x02, 0x03}))
                        .build();
        ENPA.PrioBatchHeaderSignature signature2 =
                ENPA.PrioBatchHeaderSignature.newBuilder()
                        .setBatchHeaderSignature(ByteBuffer.wrap(new byte[] {0x04, 0x05, 0x06}))
                        .build();
        signatures.add(signature1);
        signatures.add(signature2);

        File serializedSignatures = tmpFolder.newFile();
        PrioSerializer.serializeBatchHeaderSignatures(
                signatures, serializedSignatures.getAbsolutePath());
        List<ENPA.PrioBatchHeaderSignature> deserializedSignatures =
                PrioSerializer.deserializeBatchHeaderSignatures(serializedSignatures.getAbsolutePath());
        assertEquals(signatures, deserializedSignatures);
    }

    @Test
    public void testPrioDataSharePacketSerialization() throws IOException {
        List<ENPA.PrioDataSharePacket> dataSharePackets = new ArrayList<>();
        ENPA.PrioDataSharePacket dataSharePacket1 =
                ENPA.PrioDataSharePacket.newBuilder()
                        .setDeviceNonce(ByteBuffer.wrap(new byte[] {0x07, 0x08, 0x09}))
                        .setEncryptionKeyId("verysecretandsecurevalue1")
                        .setRPit(1234567890)
                        .setUuid("uniqueuserid1")
                        .setVersionConfiguration("v1.0")
                        .setEncryptedPayload(ByteBuffer.wrap(new byte[] {0x01, 0x02, 0x03, 0x04, 0x05}))
                        .build();

        ENPA.PrioDataSharePacket dataSharePacket2 =
                ENPA.PrioDataSharePacket.newBuilder()
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
        PrioSerializer.serializeDataSharePackets(
                dataSharePackets, serializedDataShares.getAbsolutePath());
        List<ENPA.PrioDataSharePacket> deserializedHeaders =
                PrioSerializer.deserializeDataSharePackets(serializedDataShares.getAbsolutePath());
        assertEquals(dataSharePackets, deserializedHeaders);
    }
}
