package org.treequery.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.treequery.Transform.QueryLeafNode;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.utils.AvroSchemaHelper;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.utils.AvroIOHelper;
import org.treequery.utils.JsonInstructionHelper;
import org.treequery.utils.TestDataAgent;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
@Tag("integration")
public class SimpleAsyncOneNodeMongoService {
    TreeQueryClusterService treeQueryClusterService = null;
    AvroSchemaHelper avroSchemaHelper = null;
    CacheTypeEnum cacheTypeEnum;
    BeamCacheOutputInterface beamCacheOutputInterface = null;
    DiscoveryServiceInterface discoveryServiceInterface = null;


    @BeforeEach
    void init() throws IOException {
        cacheTypeEnum = CacheTypeEnum.FILE;
        avroSchemaHelper = mock(AvroSchemaHelper.class);
        beamCacheOutputInterface = new TestFileBeamCacheOutputImpl();
        discoveryServiceInterface = mock(DiscoveryServiceInterface.class);
    }

    @Test
    void runAsyncSimpleMongoReadTesting() throws Exception{
        String AvroTree = "SimpleMongoReadCluster.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        assertThat(jsonString).isNotBlank();
        Node rootNode = JsonInstructionHelper.createNode(jsonString);
        assertThat(rootNode).isInstanceOf(QueryLeafNode.class);
        QueryLeafNode queryLeafNode = (QueryLeafNode)rootNode;
        when(avroSchemaHelper.getAvroSchema(rootNode)).then(
                (node)-> {
                    return queryLeafNode.getAvroSchemaObj();
                }
        );
        treeQueryClusterService =  AsyncTreeQueryClusterService.builder()
                .treeQueryClusterRunnerFactory(()->{
                    return TreeQueryClusterRunnerImpl.builder()
                            .beamCacheOutputInterface(beamCacheOutputInterface)
                            .cacheTypeEnum(cacheTypeEnum)
                            .avroSchemaHelper(avroSchemaHelper)
                            .discoveryServiceInterface(discoveryServiceInterface)
                            .build();
                })
                .build();
        treeQueryClusterService.runQueryTreeNetwork(rootNode, (status)->{
            log.debug(status.toString());
            if (status.status == StatusTreeQueryCluster.QueryTypeEnum.FAIL){
                throw new IllegalStateException(status.description);
            }
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
}
