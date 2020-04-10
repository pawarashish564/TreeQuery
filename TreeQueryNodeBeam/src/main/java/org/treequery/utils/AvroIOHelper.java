package org.treequery.utils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;

public class AvroIOHelper {

    public static void readAvroGenericRecordFile (File avroFile, Schema schema, Consumer<GenericRecord> dataConsumer) throws IOException{
        DatumReader<GenericRecord> datumReader = Optional.ofNullable(schema)
                .map(s->new GenericDatumReader<GenericRecord>(s))
                .orElse(new GenericDatumReader<GenericRecord>());
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);
        GenericRecord record = null;
        while (dataFileReader.hasNext()){
            record = dataFileReader.next(record);
            dataConsumer.accept(record);
        }
    }
    public static <T> void readAvroSpecifClassFile(File avroFile, Class c,  Consumer< T > dataConsumer) throws IOException {
        DatumReader<T> userDatumReader = new SpecificDatumReader<T>(c);
        DataFileReader<T> dataFileReader = new DataFileReader<T>(avroFile, userDatumReader);
        T record=null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
            dataConsumer.accept(record);
        }
    }
}
