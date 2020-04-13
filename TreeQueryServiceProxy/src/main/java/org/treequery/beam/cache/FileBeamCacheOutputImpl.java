package org.treequery.beam.cache;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

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
