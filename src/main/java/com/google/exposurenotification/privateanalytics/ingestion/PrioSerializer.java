package com.google.exposurenotification.privateanalytics.ingestion;

import ENPA.PrioBatchHeader;
import ENPA.PrioBatchHeaderSignature;
import ENPA.PrioDataSharePacket;
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

    public static void serializeBatchHeaderSignatures(
            List<ENPA.PrioBatchHeaderSignature> prioBatchHeaderSignatures, String pathname)
            throws IOException {
        DatumWriter<PrioBatchHeaderSignature> signatureDatumWriter =
                new SpecificDatumWriter<>(ENPA.PrioBatchHeaderSignature.class);
        DataFileWriter<PrioBatchHeaderSignature> dataFileWriter =
                new DataFileWriter<>(signatureDatumWriter);
        dataFileWriter.create(PrioBatchHeaderSignature.getClassSchema(), new File(pathname));
        for (PrioBatchHeaderSignature prioBatchHeaderSignature : prioBatchHeaderSignatures) {
            dataFileWriter.append(prioBatchHeaderSignature);
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

    public static List<ENPA.PrioBatchHeader> deserializeBatchHeaders(String pathname)
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

    public static List<PrioBatchHeaderSignature> deserializeBatchHeaderSignatures(String pathname)
            throws IOException {
        DatumReader<PrioBatchHeaderSignature> signatureDatumReader =
                new SpecificDatumReader<>(PrioBatchHeaderSignature.class);
        DataFileReader<PrioBatchHeaderSignature> dataFileReader =
                new DataFileReader<>(new File(pathname), signatureDatumReader);
        List<PrioBatchHeaderSignature> prioBatchHeaderSignatures = new ArrayList<>();
        PrioBatchHeaderSignature prioBatchHeaderSignature;
        while (dataFileReader.hasNext()) {
            prioBatchHeaderSignature = new PrioBatchHeaderSignature();
            prioBatchHeaderSignature = dataFileReader.next(prioBatchHeaderSignature);
            prioBatchHeaderSignatures.add(prioBatchHeaderSignature);
        }
        return prioBatchHeaderSignatures;
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
