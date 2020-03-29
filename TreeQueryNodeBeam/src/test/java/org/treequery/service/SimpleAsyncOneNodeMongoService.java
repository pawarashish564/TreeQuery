package org.treequery.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.model.AvroSchemaHelper;
import org.treequery.model.CacheTypeEnum;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.mockito.Mockito.mock;
@Slf4j
@Tag("integration")
public class SimpleAsyncOneNodeMongoService {
    TreeQueryClusterService treeQueryClusterService = null;
    AvroSchemaHelper avroSchemaHelper = null;
    CacheTypeEnum cacheTypeEnum;
    BeamCacheOutputInterface beamCacheOutputInterface = null;

    @BeforeEach
    void init() throws IOException {
        cacheTypeEnum = CacheTypeEnum.FILE;
        avroSchemaHelper = mock(AvroSchemaHelper.class);
        Path path = Files.createTempDirectory("TreeQuery_");
        log.debug("Write temp result into "+path.toAbsolutePath().toString());
        beamCacheOutputInterface = new TestFileBeamCacheOutputImpl();
    }

    @Test
    void runAsyncSimpleMongoReadTesting() throws Exception{
        throw new NoSuchMethodError("Not yet implemented");

    }
}
