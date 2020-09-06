package org.treequery.service;

import org.treequery.beam.cache.BeamCacheOutputInterface;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
public class TestFileBeamCacheOutputImpl implements BeamCacheOutputInterface {
    Path path;
    String fileName;
    public TestFileBeamCacheOutputImpl() throws IOException {
        path = Files.createTempDirectory("TreeQuery_");
        log.debug("Write temp result into "+path.toAbsolutePath().toString());
    }

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
