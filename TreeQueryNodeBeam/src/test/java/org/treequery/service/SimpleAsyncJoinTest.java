package org.treequery.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.Transform.JoinNode;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.model.AvroSchemaHelper;
import org.treequery.model.BasicAvroSchemaHelper;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.util.JsonInstructionHelper;
import org.treequery.utils.TestDataAgent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Slf4j
public class SimpleAsyncJoinTest {
    TreeQueryClusterService treeQueryClusterService = null;

    BeamCacheOutputInterface beamCacheOutputInterface = null;
    CacheTypeEnum cacheTypeEnum;
    AvroSchemaHelper avroSchemaHelper = null;

    @BeforeEach
    public void init() throws IOException {
        cacheTypeEnum = CacheTypeEnum.FILE;
        avroSchemaHelper = new BasicAvroSchemaHelper();
        beamCacheOutputInterface = new TestFileBeamCacheOutputImpl();
    }

    @Test
    public void SimpleAsyncJoinTestWithSameCluster() throws Exception{
        String AvroTree = "SimpleJoin.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        Node rootNode = JsonInstructionHelper.createNode(jsonString);
        assertThat(rootNode).isInstanceOf(JoinNode.class);
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
            throw new IllegalStateException(status.toString());
        });
        synchronized (rootNode){
            rootNode.wait();
        }
    }
}
