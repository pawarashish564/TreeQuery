package org.treequery.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.treequery.Transform.QueryLeafNode;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.model.AvroSchemaHelper;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.util.JsonInstructionHelper;
import org.treequery.utils.TestDataAgent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    }
}
