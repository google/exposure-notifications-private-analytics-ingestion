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
package com.google.exposurenotification.privateanalytics.ingestion.pipeline;

import com.google.exposurenotification.privateanalytics.ingestion.model.DataShare;
import com.google.exposurenotification.privateanalytics.ingestion.model.DataShare.DataShareMetadata;
import com.google.exposurenotification.privateanalytics.ingestion.model.DataShare.EncryptedShare;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.abetterinternet.prio.v1.PrioIngestionHeader;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for serializing and deserializing Prio data shares into (or from) the Apache Avro file
 * format.
 */
public class PrioSerializationHelper {

  private PrioSerializationHelper() {}

  private static final Logger LOG = LoggerFactory.getLogger(PrioSerializationHelper.class);

  public static <T extends SpecificRecordBase> ByteBuffer serializeRecords(
      List<T> records, Class<T> recordClass, Schema schema) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DatumWriter<T> dataShareDatumWriter = new SpecificDatumWriter<>(recordClass);
    try (DataFileWriter<T> dataFileWriter = new DataFileWriter<>(dataShareDatumWriter)) {
      dataFileWriter.create(schema, outputStream);

      for (T record : records) {
        dataFileWriter.append(record);
      }

      dataFileWriter.flush();
      dataFileWriter.close();
    }
    return ByteBuffer.wrap(outputStream.toByteArray());
  }

  public static <T extends SpecificRecordBase> List<T> deserializeRecords(
      Class<T> recordClass, String pathname)
      throws IOException, IllegalAccessException, InstantiationException {
    DatumReader<T> datumReader = new SpecificDatumReader<>(recordClass);
    List<T> results = new ArrayList<>();
    try (DataFileReader<T> dataFileReader = new DataFileReader<>(new File(pathname), datumReader)) {
      T record;
      while (dataFileReader.hasNext()) {
        try {
          record = recordClass.getDeclaredConstructor().newInstance();
          record = dataFileReader.next(record);
          results.add(record);
        } catch (InvocationTargetException | NoSuchMethodException e) {
          LOG.error("PrioSerializationHelper Record instance creation error:", e);
        }
      }
    }
    return results;
  }

  public static PrioIngestionHeader createHeader(
      DataShareMetadata metadata, byte[] digest, UUID uuid, long startTime, long duration) {
    return PrioIngestionHeader.newBuilder()
        .setBatchUuid(new Utf8(uuid.toString()))
        .setName(new Utf8(metadata.getMetricName()))
        .setBatchStartTime(startTime)
        .setBatchEndTime(startTime + duration)
        .setNumberOfServers(metadata.getNumberOfServers())
        .setBins(metadata.getBins())
        .setHammingWeight(metadata.getHammingWeight())
        .setPrime(metadata.getPrime())
        .setEpsilon(metadata.getEpsilon())
        .setPacketFileDigest(ByteBuffer.wrap(digest))
        .build();
  }

  public static List<PrioDataSharePacket> splitPackets(DataShare dataShare) {
    List<EncryptedShare> encryptedDataShares = dataShare.getEncryptedDataShares();
    List<PrioDataSharePacket> splitDataShares = new ArrayList<>();
    for (EncryptedShare encryptedShare : encryptedDataShares) {
      splitDataShares.add(
          PrioDataSharePacket.newBuilder()
              .setEncryptedPayload(ByteBuffer.wrap(encryptedShare.getEncryptedPayload()))
              .setEncryptionKeyId(null)
              .setRPit(dataShare.getRPit())
              .setUuid(dataShare.getUuid())
              .setVersionConfiguration(null)
              .setDeviceNonce(null)
              .build());
    }
    return splitDataShares;
  }
}
