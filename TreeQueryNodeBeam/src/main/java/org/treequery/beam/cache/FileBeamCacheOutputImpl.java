package org.treequery.beam.cache;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;
import org.treequery.utils.AvroIOHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
public class FileBeamCacheOutputImpl implements BeamCacheOutputInterface {
    Path path;
    String fileName;

    public FileBeamCacheOutputImpl(){
        try {
            this.path = Files.createTempDirectory("TreeQuery_");
            log.info("Write cache result into "+path.toAbsolutePath().toString());
        }catch(IOException ioe){
            log.error(ioe.getMessage());
            throw new IllegalStateException(String.format("File Cache failed to allocate %s",ioe.getMessage()));
        }
    }

    public FileBeamCacheOutputImpl(String path){
        this.path = Paths.get(path);
    }

    @Override
    public void writeGenericRecord(PCollection<GenericRecord> stream, Schema avroSchema, String outputLabel) {
        fileName = String.format("%s/%s", path.toAbsolutePath().toString(), outputLabel);
        stream.apply(
                AvroIO.writeGenericRecords(avroSchema).to(fileName).withoutSharding().withSuffix(".avro")
        );
    }

    public File getFile(){
        File file = new File(fileName+".avro");
        return file;
    }

    @Override
    public Schema getPageRecord(long pageSize, long page, Consumer<GenericRecord> dataConsumer) {
        Schema schema;
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        String readFileName = String.format("%s.avro",this.fileName);
        try {
            schema = AvroIOHelper.getPageRecordFromAvroFile(readFileName, pageSize, page, dataConsumer);
        }catch(IOException ioe){
            log.error(String.format("Failed to retrieve %s:%s",fileName+".avro",ioe.getMessage()));
            throw new IllegalStateException(String.format("Failed to retrieve %s:%s",fileName+".avro",ioe.getMessage()));
        }
        return schema;
    }
}
