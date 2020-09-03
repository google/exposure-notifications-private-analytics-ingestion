package com.google.exposurenotification.privateanalytics.ingestion;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tool for serializing/deserializing Prio data shares into the Apache Avro file format.
 */

public class PrioSerializer {
    public static void serializePrioBatchHeaders(List<ENPA.PrioBatchHeader> prioBatchHeaders, String pathname) throws IOException {
        DatumWriter<ENPA.PrioBatchHeader> pbhDatumWriter = new SpecificDatumWriter<ENPA.PrioBatchHeader>(ENPA.PrioBatchHeader.class);
        DataFileWriter<ENPA.PrioBatchHeader> dataFileWriter = new DataFileWriter<ENPA.PrioBatchHeader>(pbhDatumWriter);
        dataFileWriter.create(ENPA.PrioBatchHeader.getClassSchema(), new File(pathname));
        for (ENPA.PrioBatchHeader prioBatchHeader: prioBatchHeaders) {
            dataFileWriter.append(prioBatchHeader);
        }
        dataFileWriter.close();
    }

    public static List<ENPA.PrioBatchHeader> deserializePrioBatchHeaders(String pathname) throws IOException {
        DatumReader<ENPA.PrioBatchHeader> pbhDatumReader = new SpecificDatumReader<ENPA.PrioBatchHeader>(ENPA.PrioBatchHeader.class);
        DataFileReader<ENPA.PrioBatchHeader> dataFileReader = new DataFileReader<ENPA.PrioBatchHeader>(new File(pathname), pbhDatumReader);
        List<ENPA.PrioBatchHeader> prioBatchHeaders = new ArrayList<>();
        ENPA.PrioBatchHeader prioBatchHeader = null;
        while (dataFileReader.hasNext()) {
            prioBatchHeader =  new ENPA.PrioBatchHeader();
            prioBatchHeader = dataFileReader.next(prioBatchHeader);
            prioBatchHeaders.add(prioBatchHeader);
        }
        return prioBatchHeaders;
    }
}
