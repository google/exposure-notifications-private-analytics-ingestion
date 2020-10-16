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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Holder of various DoFn's for aspects of serialization */
public class SerializePacketHeaderSignature
    extends DoFn<KV<DataShareMetadata, Iterable<DataShare>>, Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(IngestionPipeline.class);
  private static int phaIndex = 0;
  private static int facilitatorIndex = 1;
  private KeyManagementServiceClient client;
  private CryptoKeyVersionName keyVersionName;

  @StartBundle
  public void startBundle(StartBundleContext context) throws IOException {
    client = KeyManagementServiceClient.create();
    IngestionPipelineOptions options =
        context.getPipelineOptions().as(IngestionPipelineOptions.class);
    keyVersionName = CryptoKeyVersionName.parse(options.getKeyResourceName().get());
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    IngestionPipelineOptions options = c.getPipelineOptions().as(IngestionPipelineOptions.class);

    String phaPrefix = options.getPHAOutput().get();
    String facilitatorPrefix = options.getFacilitatorOutput().get();
    long startTime = options.getStartTime().get();
    long duration = options.getDuration().get();

    KV<DataShareMetadata, Iterable<DataShare>> input = c.element();
    DataShareMetadata metadata = input.getKey();
    // TODO Look at the size and reject for min participant count.

    List<List<PrioDataSharePacket>> serializedDatashare = new ArrayList<>();
    for (DataShare dataShare : input.getValue()) {
      serializedDatashare.add(getPrioDataSharePackets(dataShare));
    }
    ;

    List<PrioDataSharePacket> phaPackets =
        serializedDatashare.stream()
            .map(listPacket -> listPacket.get(phaIndex))
            .collect(Collectors.toList());
    List<PrioDataSharePacket> facilitatorPackets =
        serializedDatashare.stream()
            .map(listPacket -> listPacket.get(facilitatorIndex))
            .collect(Collectors.toList());

    UUID batchId = UUID.randomUUID();
    String phaFilePath = phaPrefix + batchId.toString() + "_metric=" + metadata.getMetricName();
    // TODO pass the UUID for the full batch and not this file
    serializePacketHeaderAndSig(startTime, duration, metadata, batchId, phaFilePath, phaPackets);

    String facilitatorPath =
        facilitatorPrefix + batchId.toString() + "_metric=" + metadata.getMetricName();
    // TODO pass the UUID for the full batch and not this file
    serializePacketHeaderAndSig(
        startTime, duration, metadata, batchId, facilitatorPath, facilitatorPackets);
  }

  // TODO work on serialization to cloud storage.
  private void serializePacketHeaderAndSig(
      long startTime,
      long duration,
      DataShareMetadata metadata,
      UUID uuid,
      String filename,
      List<PrioDataSharePacket> packets) {
    try {
      // write PrioDataSharePackets in this batch to file
      String packetsFilename = filename + ".avro";
      PrioSerializer.serializeDataSharePackets(packets, packetsFilename);

      // read back PrioDataSharePacket batch file and generate digest
      String packetsFileContent = FileUtils.readFileToString(new File(packetsFilename));
      MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
      byte[] packetFileContentHash = sha256.digest(packetsFileContent.getBytes());
      Digest digest =
          Digest.newBuilder().setSha256(ByteString.copyFrom(packetFileContentHash)).build();

      // create Header and write to file
      PrioIngestionHeader header = createHeader(metadata, digest, uuid, startTime, duration);
      FileUtils.writeByteArrayToFile(new File(filename), header.toByteBuffer().array());

      // Read back header file content and generate .sig file
      String headerFileContent = FileUtils.readFileToString(new File(filename));
      String signatureFileName = filename + ".sig";
      byte[] hashHeaderFileContent = sha256.digest(headerFileContent.getBytes());
      Digest digestHeader =
          Digest.newBuilder().setSha256(ByteString.copyFrom(hashHeaderFileContent)).build();

      // TODO(amanraj): What happens if we fail an individual signing, should we fail that batch or
      // the
      // full pipeline?
      // perform signature
      AsymmetricSignResponse result = client.asymmetricSign(keyVersionName, digestHeader);
      FileUtils.writeByteArrayToFile(
          new File(signatureFileName), result.getSignature().toByteArray());
    } catch (IOException e) {
      LOG.warn("Unable to serialize or read back Packet/Header/Sig file", e);
    } catch (NoSuchAlgorithmException e) {
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

  private static List<PrioDataSharePacket> getPrioDataSharePackets(DataShare dataShare) {
    List<EncryptedShare> encryptedDataShares = dataShare.getEncryptedDataShares();
    List<PrioDataSharePacket> splitDataShares = new ArrayList<>();
    for (EncryptedShare encryptedShare : encryptedDataShares) {
      splitDataShares.add(
          PrioDataSharePacket.newBuilder()
              .setEncryptionKeyId(encryptedShare.getEncryptionKeyId())
              .setEncryptedPayload(ByteBuffer.wrap(encryptedShare.getEncryptedPayload()))
              .setRPit(dataShare.getRPit())
              .setUuid(dataShare.getUuid())
              .build());
    }
    return splitDataShares;
  }
}
