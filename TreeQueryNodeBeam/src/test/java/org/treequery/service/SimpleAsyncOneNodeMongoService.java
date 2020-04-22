package org.treequery.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.treequery.Transform.QueryLeafNode;
import org.treequery.beam.cache.BeamCacheOutputBuilder;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.*;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.beam.cache.CacheInputInterface;

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
    TreeQuerySetting treeQuerySetting = null;
    TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;
    CacheInputInterface cacheInputInterface;

    @BeforeEach
    void init() throws IOException {
        cacheTypeEnum = CacheTypeEnum.FILE;
        treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
        avroSchemaHelper = mock(AvroSchemaHelper.class);
        beamCacheOutputInterface = new TestFileBeamCacheOutputImpl();
        discoveryServiceInterface = mock(DiscoveryServiceInterface.class);
        treeQueryClusterRunnerProxyInterface = mock(TreeQueryClusterRunnerProxyInterface.class);
        cacheInputInterface = mock(CacheInputInterface.class);
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
                            .beamCacheOutputBuilder(BeamCacheOutputBuilder.builder()
                                    .cacheTypeEnum(cacheTypeEnum)
                                    .treeQuerySetting(this.treeQuerySetting)
                                    .build())
                            .cacheTypeEnum(cacheTypeEnum)
                            .avroSchemaHelper(avroSchemaHelper)
                            .treeQuerySetting(treeQuerySetting)
                            .treeQueryClusterRunnerProxyInterface(treeQueryClusterRunnerProxyInterface)
                            .cacheInputInterface(cacheInputInterface)
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
        long pageSize = 10000;
        long page = 1;
        AtomicInteger counter = new AtomicInteger();
        Schema schema = AvroIOHelper.getPageRecordFromAvroCache(
                treeQuerySetting,
                rootNode.getIdentifier(),pageSize,page,
                (record)->{
                    assertThat(record).isNotNull();
                    counter.incrementAndGet();
                });
        assertEquals(16, counter.get());
    }
}
