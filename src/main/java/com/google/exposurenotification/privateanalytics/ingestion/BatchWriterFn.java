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
import com.google.common.collect.ImmutableList;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.DataShareMetadata;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.abetterinternet.prio.v1.PrioBatchSignature;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.abetterinternet.prio.v1.PrioIngestionHeader;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function to write files (header, data records, signature) for a batch of {@link DataShare}
 *
 * <p>Outputs those DataShares that did not successfully make it into a batch file.
 */
public class BatchWriterFn extends DoFn<KV<DataShareMetadata, Iterable<DataShare>>, DataShare> {

  public static final String INGESTION_HEADER_SUFFIX = ".batch";
  public static final String DATASHARE_PACKET_SUFFIX = ".batch.avro";
  public static final String HEADER_SIGNATURE_SUFFIX = ".batch.sig.avro";

  private static final Logger LOG = LoggerFactory.getLogger(BatchWriterFn.class);
  private static final int PHA_INDEX = 0;
  private static final int FACILITATOR_INDEX = 1;
  private static final Duration KMS_WAIT_TIME = Duration.ofSeconds(30);

  private static final Counter successfulBatches =
      Metrics.counter(BatchWriterFn.class, "successfulBatches");

  private static final Counter failedBatches =
      Metrics.counter(BatchWriterFn.class, "failedBatches");

  private static final DateTimeFormatter formatter =
      DateTimeFormatter.ofPattern("/yyyy/MM/dd/HH/mm/");

  private KeyManagementServiceClient client;
  private CryptoKeyVersionName keyVersionName;

  @StartBundle
  public void startBundle(StartBundleContext context) throws IOException {
    client = KeyManagementServiceClient.create();
    IngestionPipelineOptions options =
        context.getPipelineOptions().as(IngestionPipelineOptions.class);
    keyVersionName = CryptoKeyVersionName.parse(options.getKeyResourceName());
  }

  @FinishBundle
  public void finishBundle() {
    client.shutdown();
    LOG.info("Waiting for KMS Client to shutdown.");
    try {
      client.awaitTermination(KMS_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for client shutdown", e);
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    IngestionPipelineOptions options = c.getPipelineOptions().as(IngestionPipelineOptions.class);

    String phaPrefix = options.getPHAOutput();
    String facilitatorPrefix = options.getFacilitatorOutput();
    long startTime = options.getStartTime();
    long duration = options.getDuration();

    KV<DataShareMetadata, Iterable<DataShare>> input = c.element();
    DataShareMetadata metadata = input.getKey();
    LOG.info("Processing batch: " + metadata.toString());
    // batch size explicitly chosen so that this list fits in memory on a single worker
    List<List<PrioDataSharePacket>> serializedDatashare = new ArrayList<>();
    for (DataShare dataShare : input.getValue()) {
      serializedDatashare.add(PrioSerializationHelper.splitPackets(dataShare));
    }

    List<PrioDataSharePacket> phaPackets =
        serializedDatashare.stream()
            .map(listPacket -> listPacket.get(PHA_INDEX))
            .collect(Collectors.toList());
    List<PrioDataSharePacket> facilitatorPackets =
        serializedDatashare.stream()
            .map(listPacket -> listPacket.get(FACILITATOR_INDEX))
            .collect(Collectors.toList());

    UUID batchId = UUID.randomUUID();
    String date = LocalDateTime.now(ZoneOffset.UTC).format(formatter);
    String aggregateId = "google/" + metadata.getMetricName() + date;
    String phaFilePath =
        phaPrefix + ((phaPrefix.endsWith("/")) ? "" : "/") + aggregateId + batchId.toString();
    String facilitatorPath =
        facilitatorPrefix
            + ((facilitatorPrefix.endsWith("/")) ? "" : "/")
            + aggregateId
            + batchId.toString();

    try {
      writeBatch(startTime, duration, metadata, batchId, phaFilePath, phaPackets);
      writeBatch(startTime, duration, metadata, batchId, facilitatorPath, facilitatorPackets);
      successfulBatches.inc();
    } catch (IOException | NoSuchAlgorithmException e) {
      LOG.warn("Unable to serialize Packet/Header/Sig file for PHA or facilitator", e);
      failedBatches.inc();
      input.getValue().forEach(c::output);
    }
  }

  /** Writes the triplet of files defined per batch of data shares (packet file, header, and sig) */
  private void writeBatch(
      long startTime,
      long duration,
      DataShareMetadata metadata,
      UUID uuid,
      String filenamePrefix,
      List<PrioDataSharePacket> packets)
      throws IOException, NoSuchAlgorithmException {

    // write PrioDataSharePackets in this batch to file
    ByteBuffer packetsByteBuffer =
        PrioSerializationHelper.serializeRecords(
            packets, PrioDataSharePacket.class, PrioDataSharePacket.getClassSchema());
    writeToFile(filenamePrefix + DATASHARE_PACKET_SUFFIX, packetsByteBuffer);

    MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
    byte[] packetsBytesHash = sha256.digest(packetsByteBuffer.array());
    Digest digest = Digest.newBuilder().setSha256(ByteString.copyFrom(packetsBytesHash)).build();

    // create Header and write to file
    PrioIngestionHeader header =
        PrioSerializationHelper.createHeader(metadata, digest, uuid, startTime, duration);
    ByteBuffer headerBytes =
        PrioSerializationHelper.serializeRecords(
            ImmutableList.of(header),
            PrioIngestionHeader.class,
            PrioIngestionHeader.getClassSchema());
    writeToFile(filenamePrefix + INGESTION_HEADER_SUFFIX, headerBytes);

    byte[] hashHeader = sha256.digest(header.toByteBuffer().array());
    Digest digestHeader = Digest.newBuilder().setSha256(ByteString.copyFrom(hashHeader)).build();

    AsymmetricSignResponse result = client.asymmetricSign(keyVersionName, digestHeader);
    PrioBatchSignature signature =
        PrioBatchSignature.newBuilder()
            .setBatchHeaderSignature(result.getSignature().asReadOnlyByteBuffer())
            .setKeyIdentifier(keyVersionName.toString())
            .build();
    ByteBuffer signatureBytes =
        PrioSerializationHelper.serializeRecords(
            ImmutableList.of(signature),
            PrioBatchSignature.class,
            PrioBatchSignature.getClassSchema());
    writeToFile(filenamePrefix + HEADER_SIGNATURE_SUFFIX, signatureBytes);
  }

  static void writeToFile(String filename, ByteBuffer contents) throws IOException {
    LOG.info("Writing output file: " + filename);
    ResourceId resourceId = FileSystems.matchNewResource(filename, false);
    try (WritableByteChannel out = FileSystems.create(resourceId, MimeTypes.TEXT)) {
      out.write(contents);
    }
  }
}
