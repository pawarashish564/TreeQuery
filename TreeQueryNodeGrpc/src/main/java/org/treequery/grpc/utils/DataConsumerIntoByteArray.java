package org.treequery.grpc.utils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Consumer;
@Slf4j
public class DataConsumerIntoByteArray implements Consumer<GenericRecord> {
    DatumWriter<GenericRecord> recordDatumWriter;
    ByteArrayOutputStream byteArrayOutputStream;
    BinaryEncoder binaryEncoder;
    @Getter
    int dataSize=0;
    public DataConsumerIntoByteArray(Schema outputSchema){
        recordDatumWriter = new GenericDatumWriter<GenericRecord>(outputSchema);
        byteArrayOutputStream = new ByteArrayOutputStream();
        binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
    }


    @Override
    public void accept(GenericRecord genericRecord) {
        try {
            recordDatumWriter.write(genericRecord, binaryEncoder);
            binaryEncoder.flush();
            dataSize++;
        }catch(IOException ioe){
            log.error(ioe.getMessage());
            throw new IllegalStateException("Failed to write Generic Record to Bytes");
        }
    }

    public byte[] toArrayOutput(){
        byte [] byteData = byteArrayOutputStream.toByteArray();
        try {
            byteArrayOutputStream.close();
        }catch(IOException ioe){}
        return byteData;
    }

}
