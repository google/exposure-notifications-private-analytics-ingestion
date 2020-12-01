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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
  public static final String HEADER_SIGNATURE_SUFFIX = ".batch.sig";

  private static final Logger LOG = LoggerFactory.getLogger(BatchWriterFn.class);
  private static final Duration KMS_WAIT_TIME = Duration.ofSeconds(30);
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("/yyyy/MM/dd/HH/mm/");

  private static final Counter successfulBatches =
      Metrics.counter(BatchWriterFn.class, "successfulBatches");

  private static final Counter failedBatches =
      Metrics.counter(BatchWriterFn.class, "failedBatches");

  private KeyManagementServiceClient client;
  private CryptoKeyVersionName keyVersionName;

  // Uses pipeline options, otherwise could've lived in @Setup
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

    String phaPrefix = options.getPhaOutput();
    String facilitatorPrefix = options.getFacilitatorOutput();

    long startTime =
        IngestionPipelineOptions.calculatePipelineStart(
            options.getStartTime(), options.getDuration(), Clock.systemUTC());
    long duration = options.getDuration();

    KV<DataShareMetadata, Iterable<DataShare>> input = c.element();
    DataShareMetadata metadata = input.getKey();
    LOG.info("Processing batch: " + metadata.toString());
    // batch size explicitly chosen so that these lists fit in memory on a single worker
    List<PrioDataSharePacket> phaPackets = new ArrayList<>();
    List<PrioDataSharePacket> facilitatorPackets = new ArrayList<>();
    for (DataShare dataShare : input.getValue()) {
      List<PrioDataSharePacket> split = PrioSerializationHelper.splitPackets(dataShare);
      if (split.size() != DataShare.NUMBER_OF_SERVERS) {
        throw new IllegalArgumentException(
            "Share split into more than hardcoded number of servers");
      }
      // First packet always goes to PHA
      phaPackets.add(split.get(0));
      facilitatorPackets.add(split.get(1));
    }

    String date =
        Instant.ofEpochSecond(startTime + duration)
            .atOffset(ZoneOffset.UTC)
            .format(DATE_TIME_FORMATTER);
    String aggregateId = metadata.getMetricName() + date;
    // In case of dataflow runner retries, its useful to make the batch UUID deterministic so
    // that files that may already have been written are overwritten, instead of new files created.
    byte[] seed = (aggregateId + metadata.getBatchNumber()).getBytes();
    UUID batchId = UUID.nameUUIDFromBytes(seed);
    String phaFilePath =
        phaPrefix + ((phaPrefix.endsWith("/")) ? "" : "/") + aggregateId + batchId.toString();
    String facilitatorPath =
        facilitatorPrefix
            + ((facilitatorPrefix.endsWith("/")) ? "" : "/")
            + aggregateId
            + batchId.toString();

    try {
      // Write to PHA Output Destination
      LOG.info("PHA Output: " + phaFilePath);
      writeBatch(
          options,
          startTime,
          duration,
          metadata,
          batchId,
          phaFilePath,
          phaPackets,
          options.getPhaAwsBucketRole(),
          options.getPhaAwsBucketRegion());

      // Write to Facilitator Output Destination
      LOG.info("Facilitator Output: " + facilitatorPath);
      writeBatch(
          options,
          startTime,
          duration,
          metadata,
          batchId,
          facilitatorPath,
          facilitatorPackets,
          options.getFacilitatorAwsBucketRole(),
          options.getFacilitatorAwsBucketRegion());
      successfulBatches.inc();
    } catch (IOException | NoSuchAlgorithmException e) {
      LOG.warn("Unable to serialize Packet/Header/Sig file for PHA or facilitator", e);
      failedBatches.inc();
      input.getValue().forEach(c::output);
    }
  }

  /** Writes the triplet of files defined per batch of data shares (packet file, header, and sig) */
  private void writeBatch(
      IngestionPipelineOptions options,
      long startTime,
      long duration,
      DataShareMetadata metadata,
      UUID uuid,
      String filenamePrefix,
      List<PrioDataSharePacket> packets,
      String awsBucketRole,
      String awsBucketRegion)
      throws IOException, NoSuchAlgorithmException {

    if (filenamePrefix.startsWith("s3://")) {
      AWSFederatedAuthHelper.setupAWSAuth(options, awsBucketRole, awsBucketRegion);
      FileSystems.setDefaultPipelineOptions(options);
    }
    // write PrioDataSharePackets in this batch to file
    ByteBuffer packetsByteBuffer =
        PrioSerializationHelper.serializeRecords(
            packets, PrioDataSharePacket.class, PrioDataSharePacket.getClassSchema());
    writeToFile(filenamePrefix + DATASHARE_PACKET_SUFFIX, packetsByteBuffer);

    MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
    byte[] packetsBytesHashDigest = sha256.digest(packetsByteBuffer.array());
    // create Header and write to file
    PrioIngestionHeader header =
        PrioSerializationHelper.createHeader(
            metadata, packetsBytesHashDigest, uuid, startTime, duration);

    ByteBuffer headerBytes =
        PrioSerializationHelper.serializeRecords(
            ImmutableList.of(header),
            PrioIngestionHeader.class,
            PrioIngestionHeader.getClassSchema());
    writeToFile(filenamePrefix + INGESTION_HEADER_SUFFIX, headerBytes);

    byte[] hashHeader = sha256.digest(headerBytes.array());
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
