package org.treequery.service;

import org.treequery.Transform.LoadLeafNode;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.model.AvroSchemaHelper;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.utils.AvroIOHelper;
import org.treequery.utils.JsonInstructionHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.utils.TestDataAgent;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
@Slf4j
class SimpleAsyncOneNodeTextServiceTest {

    TreeQueryClusterService treeQueryClusterService = null;

    BeamCacheOutputInterface beamCacheOutputInterface = null;
    CacheTypeEnum cacheTypeEnum;
    AvroSchemaHelper avroSchemaHelper = null;
    @BeforeEach
    void init() throws IOException {
        cacheTypeEnum = CacheTypeEnum.FILE;
        avroSchemaHelper = mock(AvroSchemaHelper.class);
        Path path = Files.createTempDirectory("TreeQuery_");
        log.debug("Write temp result into "+path.toAbsolutePath().toString());
        beamCacheOutputInterface = new TestFileBeamCacheOutputImpl();
    }

    @Test
    void runAsyncSimpleAvroStaticReadTesting() throws Exception{
        String AvroTree = "SimpleAvroReadStaticCluster.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        Node rootNode = JsonInstructionHelper.createNode(jsonString);
        LoadLeafNode d = (LoadLeafNode) rootNode;
        when(avroSchemaHelper.getAvroSchema(rootNode)).then(
                (node)-> {
                    return d.getAvroSchemaObj();
                }
        );
        treeQueryClusterService =  AsyncTreeQueryClusterService.builder()
                .treeQueryClusterRunnerFactory(()->{
                    return TreeQueryClusterRunnerImpl.builder()
                            .beamCacheOutputInterface(beamCacheOutputInterface)
                            .cacheTypeEnum(cacheTypeEnum)
                            .avroSchemaHelper(avroSchemaHelper)
                            .build();
                })
                .build();

        treeQueryClusterService.runQueryTreeNetwork(rootNode, (status)->{
            log.debug(status.toString());
            synchronized (rootNode) {
                rootNode.notify();
            }
            assertThat(status.status).isEqualTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
        });
        synchronized (rootNode){
            rootNode.wait();
        }
        //Check the avro file
        TestFileBeamCacheOutputImpl testFileBeamCacheOutput = (TestFileBeamCacheOutputImpl) beamCacheOutputInterface;
        File avroOutputFile = testFileBeamCacheOutput.getFile();
        AtomicInteger counter = new AtomicInteger();
        AvroIOHelper.readAvroGenericRecordFile(avroOutputFile,avroSchemaHelper.getAvroSchema(rootNode),
                (record)->{
                    assertThat(record).isNotNull();
                    counter.incrementAndGet();
                });
        assertEquals(16, counter.get());
    }

    @Test
    void runAsyncSimpleAvroTradeReadTesting() throws Exception{
        String AvroTree = "SimpleAvroReadCluster.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        Node rootNode = JsonInstructionHelper.createNode(jsonString);
        LoadLeafNode d = (LoadLeafNode) rootNode;
        when(avroSchemaHelper.getAvroSchema(rootNode)).then(
                (node)-> {
                    return d.getAvroSchemaObj();
                }
        );

        treeQueryClusterService =  AsyncTreeQueryClusterService.builder()
                .treeQueryClusterRunnerFactory(()->{
                    return TreeQueryClusterRunnerImpl.builder()
                            .beamCacheOutputInterface(beamCacheOutputInterface)
                            .cacheTypeEnum(cacheTypeEnum)
                            .avroSchemaHelper(avroSchemaHelper)
                            .build();
                })
                .build();

        treeQueryClusterService.runQueryTreeNetwork(rootNode, (status)->{
            log.debug(status.toString());
            synchronized (rootNode) {
                rootNode.notify();
            }
            assertThat(status.status).isEqualTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
        });
        synchronized (rootNode){
            rootNode.wait();
        }
        //Check the avro file
        TestFileBeamCacheOutputImpl testFileBeamCacheOutput = (TestFileBeamCacheOutputImpl) beamCacheOutputInterface;
        File avroOutputFile = testFileBeamCacheOutput.getFile();
        AtomicInteger counter = new AtomicInteger();
        AvroIOHelper.readAvroGenericRecordFile(avroOutputFile,avroSchemaHelper.getAvroSchema(rootNode),
                (record)->{
                    assertThat(record).isNotNull();
                    counter.incrementAndGet();
                });
        assertEquals(1000, counter.get());
    }

}