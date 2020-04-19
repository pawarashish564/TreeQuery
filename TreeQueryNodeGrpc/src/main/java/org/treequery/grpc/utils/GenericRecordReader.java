package org.treequery.grpc.utils;

import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.util.function.Consumer;

public class GenericRecordReader {
    public static void readGenericRecordFromProtoByteString(ByteString dataLoadString, Schema schema, Consumer<GenericRecord> dataConsumer) throws IOException {
        byte[] dataLoad = dataLoadString.toByteArray();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(dataLoad, null);
        while (!decoder.isEnd()){
            GenericRecord genericRecord = datumReader.read(null, decoder);
            dataConsumer.accept(genericRecord);
        }
    }
}
