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

import com.google.cloud.kms.v1.Digest;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.DataShareMetadata;
import com.google.exposurenotification.privateanalytics.ingestion.DataShare.EncryptedShare;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.abetterinternet.prio.v1.PrioIngestionHeader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Helpers for serializing and deserializing Prio data shares into (or from) the Apache Avro file
 * format.
 */
public class PrioSerializationHelper {
  public static void serializeIngestionHeaders(
      List<PrioIngestionHeader> ingestionHeaders, String pathname) throws IOException {
    DatumWriter<PrioIngestionHeader> ingestionHeaderDatumWriter =
        new SpecificDatumWriter<>(PrioIngestionHeader.class);
    DataFileWriter<PrioIngestionHeader> dataFileWriter =
        new DataFileWriter<>(ingestionHeaderDatumWriter);
    dataFileWriter.create(PrioIngestionHeader.getClassSchema(), new File(pathname));
    for (PrioIngestionHeader ingestionHeader : ingestionHeaders) {
      dataFileWriter.append(ingestionHeader);
    }
    dataFileWriter.close();
  }

  public static void serializeDataSharePackets(
      List<PrioDataSharePacket> prioDataSharePackets, String pathname) throws IOException {
    DatumWriter<PrioDataSharePacket> dataShareDatumWriter =
        new SpecificDatumWriter<>(PrioDataSharePacket.class);
    DataFileWriter<PrioDataSharePacket> dataFileWriter = new DataFileWriter<>(dataShareDatumWriter);
    dataFileWriter.create(PrioDataSharePacket.getClassSchema(), new File(pathname));
    for (PrioDataSharePacket prioDataSharePacket : prioDataSharePackets) {
      dataFileWriter.append(prioDataSharePacket);
    }
    dataFileWriter.close();
  }

  public static ByteBuffer serializeDataSharePackets(
      List<PrioDataSharePacket> prioDataSharePackets) throws IOException {

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DatumWriter<PrioDataSharePacket> dataShareDatumWriter =
        new SpecificDatumWriter<>(PrioDataSharePacket.class);
    DataFileWriter<PrioDataSharePacket> dataFileWriter = new DataFileWriter<>(dataShareDatumWriter);
    dataFileWriter.create(PrioDataSharePacket.getClassSchema(), outputStream);
    for (PrioDataSharePacket prioDataSharePacket : prioDataSharePackets) {
      dataFileWriter.append(prioDataSharePacket);
    }
    dataFileWriter.flush();
    dataFileWriter.close();
    return ByteBuffer.wrap(outputStream.toByteArray());
  }

  public static List<PrioIngestionHeader> deserializeIngestionHeaders(String pathname)
      throws IOException {
    DatumReader<PrioIngestionHeader> ingestionHeaderDatumReader =
        new SpecificDatumReader<>(PrioIngestionHeader.class);
    DataFileReader<PrioIngestionHeader> dataFileReader =
        new DataFileReader<>(new File(pathname), ingestionHeaderDatumReader);
    List<PrioIngestionHeader> ingestionHeaders = new ArrayList<>();
    PrioIngestionHeader ingestionHeader;
    while (dataFileReader.hasNext()) {
      ingestionHeader = new PrioIngestionHeader();
      ingestionHeader = dataFileReader.next(ingestionHeader);
      ingestionHeaders.add(ingestionHeader);
    }
    return ingestionHeaders;
  }

  public static List<PrioDataSharePacket> deserializeDataSharePackets(String pathname)
      throws IOException {
    DatumReader<PrioDataSharePacket> dataShareDatumReader =
        new SpecificDatumReader<>(PrioDataSharePacket.class);
    DataFileReader<PrioDataSharePacket> dataFileReader =
        new DataFileReader<>(new File(pathname), dataShareDatumReader);
    List<PrioDataSharePacket> prioDataSharePackets = new ArrayList<>();
    PrioDataSharePacket prioDataSharePacket;
    while (dataFileReader.hasNext()) {
      prioDataSharePacket = new PrioDataSharePacket();
      prioDataSharePacket = dataFileReader.next(prioDataSharePacket);
      prioDataSharePackets.add(prioDataSharePacket);
    }
    return prioDataSharePackets;
  }

  public static PrioIngestionHeader createHeader(
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

  public static List<PrioDataSharePacket> splitPackets(DataShare dataShare) {
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
