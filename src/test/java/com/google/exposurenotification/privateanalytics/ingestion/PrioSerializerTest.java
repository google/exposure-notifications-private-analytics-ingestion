package com.google.exposurenotification.privateanalytics.ingestion;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PrioSerializer}.
 */

@RunWith(JUnit4.class)
public class PrioSerializerTest{

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void testPrioBatchHeaderSerialization() throws IOException {
        List<ENPA.PrioBatchHeader> prioBatchHeaders = new ArrayList<>();
        ENPA.PrioBatchHeader pbh1 = ENPA.PrioBatchHeader.newBuilder()
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
        ENPA.PrioBatchHeader pbh2 = ENPA.PrioBatchHeader.newBuilder()
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

        prioBatchHeaders.add(pbh1);
        prioBatchHeaders.add(pbh2);
        File serializedHeaders = tmpFolder.newFile();
        PrioSerializer.serializePrioBatchHeaders(prioBatchHeaders, serializedHeaders.getAbsolutePath());
        List<ENPA.PrioBatchHeader> deserializedHeaders = PrioSerializer.deserializePrioBatchHeaders(serializedHeaders.getAbsolutePath());
        assertEquals(prioBatchHeaders, deserializedHeaders);
    }
}
