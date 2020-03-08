package io.exp.treequery.beam;

import io.exp.treequery.execute.CacheInputInterface;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

class BeamPipelineBuilderImplTest {
    CacheInputInterface cacheInputInterface;
    String fileName = "bondtrade1.avro";
    @BeforeEach
    void init(){

        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File jsonFile = new File(classLoader.getResource(fileName).getFile());
        String directoryName = jsonFile.getParent();

        cacheInputInterface = FileCacheInputImpl.builder()
                                .fileDirectory(directoryName).build();

    }

    @Test
    void simpleAvroReadPipeline() {

    }
}