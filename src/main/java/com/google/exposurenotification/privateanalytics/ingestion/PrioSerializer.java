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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.abetterinternet.prio.v1.PrioIngestionHeader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Tool for serializing and deserializing Prio data shares into (or from) the Apache Avro file
 * format.
 */
public class PrioSerializer {
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
}
