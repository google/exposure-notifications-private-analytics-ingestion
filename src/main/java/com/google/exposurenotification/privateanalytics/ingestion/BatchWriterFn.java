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
import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Function to write files (header, data records, signature) for a batch of {@link DataShare} */
public class BatchWriterFn
    extends DoFn<KV<DataShareMetadata, Iterable<DataShare>>, Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(BatchWriterFn.class);
  private static final int phaIndex = 0;
  private static final int facilitatorIndex = 1;

  private static final Counter batchesFailingMinParticipant =
      Metrics.counter(BatchWriterFn.class, "batchesFailingMinParticipant");

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
    // batch size explicitly chosen so that this list fits in memory on a single worker
    List<List<PrioDataSharePacket>> serializedDatashare = new ArrayList<>();
    for (DataShare dataShare : input.getValue()) {
      serializedDatashare.add(PrioSerializationHelper.splitPackets(dataShare));
    }
    if (serializedDatashare.size() < options.getMinimumParticipantCount().get()) {
      LOG.warn("skipping batch of datashares for min participation");
      batchesFailingMinParticipant.inc();
      return;
    }

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
    writeBatch(startTime, duration, metadata, batchId, phaFilePath, phaPackets);

    String facilitatorPath =
        facilitatorPrefix + batchId.toString() + "_metric=" + metadata.getMetricName();
    writeBatch(
        startTime, duration, metadata, batchId, facilitatorPath, facilitatorPackets);
    c.output(true);
  }

  private void writeBatch(
      long startTime,
      long duration,
      DataShareMetadata metadata,
      UUID uuid,
      String filename,
      List<PrioDataSharePacket> packets) {
    try {
      // write PrioDataSharePackets in this batch to file
      String packetsFilename = filename + ".avro";
      PrioSerializationHelper.serializeDataSharePackets(packets, packetsFilename);

      // read back PrioDataSharePacket batch file and generate digest
      String packetsFileContent = FileUtils.readFileToString(new File(packetsFilename));
      MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
      byte[] packetFileContentHash = sha256.digest(packetsFileContent.getBytes());
      Digest digest =
          Digest.newBuilder().setSha256(ByteString.copyFrom(packetFileContentHash)).build();

      // create Header and write to file
      PrioIngestionHeader header = PrioSerializationHelper
          .createHeader(metadata, digest, uuid, startTime, duration);
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
}
