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

import com.google.cloud.kms.v1.AsymmetricSignResponse;
import com.google.cloud.kms.v1.CryptoKeyVersionName;
import com.google.cloud.kms.v1.Digest;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.DataShareMetadata;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.EncryptedShare;
import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.abetterinternet.prio.v1.PrioIngestionHeader;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holder of various DoFn's for aspects of serialization
 */
public class SerializationFunctions {
    private static final Logger LOG = LoggerFactory.getLogger(IngestionPipeline.class);

    public static class SerializeDataShareFn extends DoFn<DataShare,
        KV<DataShareMetadata, List<PrioDataSharePacket>>> {
        private static final Logger LOG = LoggerFactory.getLogger(SerializeDataShareFn.class);
        private final Counter dataShareIncluded;

        public SerializeDataShareFn(String metric) {
            this.dataShareIncluded = Metrics.counter(SerializeDataShareFn.class, "dataShareIncluded_" + metric);
        }
        @ProcessElement
        public void processElement(ProcessContext c) {
            List<EncryptedShare> encryptedDataShares = c.element().getEncryptedDataShares();
            List<PrioDataSharePacket> splitDataShares = new ArrayList<>();
            for (EncryptedShare dataShare : encryptedDataShares) {
                splitDataShares.add(
                    PrioDataSharePacket.newBuilder()
                        .setEncryptionKeyId(dataShare.getEncryptionKeyId())
                        .setEncryptedPayload(
                            ByteBuffer.wrap(
                                dataShare.getEncryptedPayload()))
                        .setRPit(c.element().getRPit())
                        .setUuid(c.element().getUuid())
                        .build()
                );
            }
            dataShareIncluded.inc();
            c.output(KV.of(c.element().getDataShareMetadata(), splitDataShares));
        }
    }

    public static class SerializePacketHeaderSignature extends
        DoFn<KV<DataShareMetadata, List<List<PrioDataSharePacket>>>, Boolean> {
        private static int phaForkIndex = 0;
        private static int facilitatorForkIndex = 1;

        private final ValueProvider<String> phaFilePrefix;
        private final ValueProvider<String> facilitatorFilePrefix;
        private final ValueProvider<Long> startTime;
        private final ValueProvider<Long> duration;
        private KeyManagementServiceClient client;
        private CryptoKeyVersionName keyVersionName;

        @StartBundle
        public void startBundle(StartBundleContext context) throws IOException {
            client = KeyManagementServiceClient.create();
            IngestionPipelineOptions options = context.getPipelineOptions().as(IngestionPipelineOptions.class);
            keyVersionName = CryptoKeyVersionName.parse(options.getKeyResourceName().get());
        }

        protected SerializePacketHeaderSignature(
            ValueProvider<String> phaFilenamePrefix,
            ValueProvider<String> facilitatorFilenamePrefix,
            ValueProvider<Long> startTime,
            ValueProvider<Long> duration) {
            this.phaFilePrefix = phaFilenamePrefix;
            this.facilitatorFilePrefix = facilitatorFilenamePrefix;
            this.startTime = startTime;
            this.duration = duration;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<DataShareMetadata, List<List<PrioDataSharePacket>>> input = c.element();
            //TODO Look at the size and reject for min participant count.
            UUID batchId = UUID.randomUUID();

            List<PrioDataSharePacket> phaPackets = input.getValue().stream()
                .map(listPacket -> listPacket.get(phaForkIndex))
                .collect(Collectors.toList());
            List<PrioDataSharePacket> facilitatorPackets = input.getValue().stream()
                .map(listPacket -> listPacket.get(facilitatorForkIndex))
                .collect(Collectors.toList());

            String phaFilePath =
                phaFilePrefix.get()
                    + batchId.toString() + "_metric=" + input.getKey().getMetricName();
            //TODO pass the UUID for the full batch and not this file
            serializePacketHeaderAndSig(input.getKey(), batchId, phaFilePath, phaPackets);

            String facilitatorFilePath =
                facilitatorFilePrefix.get()
                    + batchId.toString() + "_metric=" + input.getKey().getMetricName();
            //TODO pass the UUID for the full batch and not this file
            serializePacketHeaderAndSig(input.getKey(), batchId, facilitatorFilePath, facilitatorPackets);
        }

        //TODO work on serialization to cloud storage.
        private void serializePacketHeaderAndSig(
            DataShareMetadata metadata,
            UUID uuid,
            String filename,
            List<PrioDataSharePacket> packets) {
            try {
                //write PrioDataSharePackets in this batch to file
                String packetsFilename = filename + ".avro";
                PrioSerializer.serializeDataSharePackets(packets, packetsFilename);

                //read back PrioDataSharePacket batch file and generate digest
                String packetsFileContent = FileUtils.readFileToString(new File(packetsFilename));
                MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
                byte[] packetFileContentHash = sha256.digest(packetsFileContent.getBytes());
                Digest digest = Digest.newBuilder().setSha256(
                    ByteString.copyFrom(packetFileContentHash)).build();

                //create Header and write to file
                PrioIngestionHeader header =
                    createHeader(metadata, digest, uuid, startTime.get(), duration.get());
                FileUtils.writeByteArrayToFile(new File(filename), header.toByteBuffer().array());

                //Read back header file content and generate .sig file
                String headerFileContent = FileUtils.readFileToString(new File(filename));
                String signatureFileName = filename + ".sig";
                byte[] hashHeaderFileContent = sha256.digest(headerFileContent.getBytes());
                Digest digestHeader = Digest.newBuilder()
                    .setSha256(ByteString.copyFrom(hashHeaderFileContent)).build();

                //TODO(amanraj): What happens if we fail an individual signing, should we fail that batch or the
                // full pipeline?
                //perform signature
                AsymmetricSignResponse result = client.asymmetricSign(keyVersionName, digestHeader);
                FileUtils.writeByteArrayToFile(
                    new File(signatureFileName), result.getSignature().toByteArray());
            }
            catch (IOException e) {
                LOG.warn("Unable to serialize or read back Packet/Header/Sig file", e);
            }
            catch (NoSuchAlgorithmException e) {
                LOG.warn("Message Digest with SHA256 does not exist.", e);
            }
        }

        private static PrioIngestionHeader createHeader(
            DataShareMetadata metadata, Digest digest, UUID uuid, long startTime, long duration) {
            return PrioIngestionHeader.newBuilder()
                .setBatchUuid(uuid.toString())
                .setName("BatchUuid=" + uuid.toString())
                .setBatchStartTime(startTime)
                .setBatchEndTime(startTime + duration)
                .setNumberOfServers(metadata.getNumberOfServers())
                .setBins(metadata.getBins())
                .setHammingWeight(metadata.getHammingWeight())
                .setPrime(metadata.getPrime())
                .setEpsilon(metadata.getEpsilon())
                .setPacketFileDigest(ByteBuffer.wrap(digest.toByteArray()))
                .build();
        }
    }
}