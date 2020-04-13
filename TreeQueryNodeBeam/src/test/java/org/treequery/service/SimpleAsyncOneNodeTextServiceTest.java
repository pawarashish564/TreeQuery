package org.treequery.service;

import org.apache.avro.Schema;
import org.treequery.Transform.LoadLeafNode;
import org.treequery.beam.cache.BeamCacheOutputBuilder;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.*;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
@Slf4j
class SimpleAsyncOneNodeTextServiceTest {

    TreeQueryClusterService treeQueryClusterService = null;


    CacheTypeEnum cacheTypeEnum;
    AvroSchemaHelper avroSchemaHelper = null;
    DiscoveryServiceInterface discoveryServiceInterface = null;
    TreeQuerySetting treeQuerySetting = null;
    TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;

    @BeforeEach
    void init() throws IOException {
        cacheTypeEnum = CacheTypeEnum.FILE;
        treeQuerySetting = SettingInitializer.createTreeQuerySetting();
        avroSchemaHelper = mock(AvroSchemaHelper.class);

        discoveryServiceInterface = mock(DiscoveryServiceInterface.class);
        treeQueryClusterRunnerProxyInterface = mock(TreeQueryClusterRunnerProxyInterface.class);
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
                            .beamCacheOutputBuilder(BeamCacheOutputBuilder.builder()
                                    .cacheTypeEnum(cacheTypeEnum)
                                    .treeQuerySetting(this.treeQuerySetting)
                                    .build())
                            .cacheTypeEnum(cacheTypeEnum)
                            .avroSchemaHelper(avroSchemaHelper)
                            .atCluster(treeQuerySetting.getCluster())
                            .treeQueryClusterRunnerProxyInterface(treeQueryClusterRunnerProxyInterface)
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
        long pageSize = 10000;
        long page = 1;
        AtomicInteger counter = new AtomicInteger();
        Schema schema = AvroIOHelper.getPageRecordFromAvroCache(this.cacheTypeEnum,
                treeQuerySetting,
                rootNode.getIdentifier(),pageSize,page,
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
                            .beamCacheOutputBuilder(BeamCacheOutputBuilder.builder()
                                    .cacheTypeEnum(cacheTypeEnum)
                                    .treeQuerySetting(this.treeQuerySetting)
                                    .build())
                            .cacheTypeEnum(cacheTypeEnum)
                            .avroSchemaHelper(avroSchemaHelper)
                            .atCluster(treeQuerySetting.getCluster())
                            .treeQueryClusterRunnerProxyInterface(treeQueryClusterRunnerProxyInterface)
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
        long pageSize = 10000;
        long page = 1;
        AtomicInteger counter = new AtomicInteger();
        Schema schema = AvroIOHelper.getPageRecordFromAvroCache(this.cacheTypeEnum,
                treeQuerySetting,
                rootNode.getIdentifier(),pageSize,page,
                (record)->{
                    assertThat(record).isNotNull();
                    counter.incrementAndGet();
                });
        assertEquals(1000, counter.get());
    }

}