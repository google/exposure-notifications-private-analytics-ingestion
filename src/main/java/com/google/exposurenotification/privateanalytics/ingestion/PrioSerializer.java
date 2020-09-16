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

import org.abetterinternet.prio.v1.PrioBatchHeader;
import org.abetterinternet.prio.v1.PrioIngestionSignature;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
    public static void serializeBatchHeaders(List<PrioBatchHeader> prioBatchHeaders, String pathname)
            throws IOException {
        DatumWriter<PrioBatchHeader> batchHeaderDatumWriter =
                new SpecificDatumWriter<>(PrioBatchHeader.class);
        DataFileWriter<PrioBatchHeader> dataFileWriter = new DataFileWriter<>(batchHeaderDatumWriter);
        dataFileWriter.create(PrioBatchHeader.getClassSchema(), new File(pathname));
        for (PrioBatchHeader prioBatchHeader : prioBatchHeaders) {
            dataFileWriter.append(prioBatchHeader);
        }
        dataFileWriter.close();
    }

    public static void serializeIngestionSignatures(
            List<PrioIngestionSignature> prioIngestionSignatures, String pathname)
            throws IOException {
        DatumWriter<PrioIngestionSignature> signatureDatumWriter =
                new SpecificDatumWriter<>(PrioIngestionSignature.class);
        DataFileWriter<PrioIngestionSignature> dataFileWriter =
                new DataFileWriter<>(signatureDatumWriter);
        dataFileWriter.create(PrioIngestionSignature.getClassSchema(), new File(pathname));
        for (PrioIngestionSignature prioIngestionSignature : prioIngestionSignatures) {
            dataFileWriter.append(prioIngestionSignature);
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

    public static List<PrioBatchHeader> deserializeBatchHeaders(String pathname)
            throws IOException {
        DatumReader<PrioBatchHeader> batchHeaderDatumReader =
                new SpecificDatumReader<>(PrioBatchHeader.class);
        DataFileReader<PrioBatchHeader> dataFileReader =
                new DataFileReader<>(new File(pathname), batchHeaderDatumReader);
        List<PrioBatchHeader> prioBatchHeaders = new ArrayList<>();
        PrioBatchHeader prioBatchHeader;
        while (dataFileReader.hasNext()) {
            prioBatchHeader = new PrioBatchHeader();
            prioBatchHeader = dataFileReader.next(prioBatchHeader);
            prioBatchHeaders.add(prioBatchHeader);
        }
        return prioBatchHeaders;
    }

    public static List<PrioIngestionSignature> deserializeIngestionSignatures(String pathname)
            throws IOException {
        DatumReader<PrioIngestionSignature> signatureDatumReader =
                new SpecificDatumReader<>(PrioIngestionSignature.class);
        DataFileReader<PrioIngestionSignature> dataFileReader =
                new DataFileReader<>(new File(pathname), signatureDatumReader);
        List<PrioIngestionSignature> prioIngestionSignatures = new ArrayList<>();
        PrioIngestionSignature prioIngestionSignature;
        while (dataFileReader.hasNext()) {
            prioIngestionSignature = new PrioIngestionSignature();
            prioIngestionSignature = dataFileReader.next(prioIngestionSignature);
            prioIngestionSignatures.add(prioIngestionSignature);
        }
        return prioIngestionSignatures;
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
