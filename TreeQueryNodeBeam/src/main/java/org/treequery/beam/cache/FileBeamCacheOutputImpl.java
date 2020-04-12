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
import java.util.Optional;
import java.util.function.Consumer;

@Slf4j
public class FileBeamCacheOutputImpl implements BeamCacheOutputInterface {
    Path path;
    String fileName;

    /*
    public FileBeamCacheOutputImpl(){
        try {
            this.path = Files.createTempDirectory("TreeQuery_");
            log.info("Write cache result into "+path.toAbsolutePath().toString());
        }catch(IOException ioe){
            log.error(ioe.getMessage());
            throw new IllegalStateException(String.format("File Cache failed to allocate %s",ioe.getMessage()));
        }
    }*/

    public FileBeamCacheOutputImpl(String path){
        this.path = Paths.get(Optional.ofNullable(path).orElseThrow(()->new IllegalArgumentException("Cache output cannot be null")));
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

}
