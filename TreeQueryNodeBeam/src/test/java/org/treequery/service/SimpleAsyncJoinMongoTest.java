package org.treequery.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.treequery.Transform.JoinNode;
import org.treequery.beam.cache.BeamCacheOutputBuilder;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.*;
import org.treequery.utils.BasicAvroSchemaHelperImpl;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

@Slf4j
@Tag("integration")
public class SimpleAsyncJoinMongoTest {
    TreeQueryClusterService treeQueryClusterService = null;


    CacheTypeEnum cacheTypeEnum;
    AvroSchemaHelper avroSchemaHelper = null;
    DiscoveryServiceInterface discoveryServiceInterface = null;
    TreeQuerySetting treeQuerySetting = null;
    TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;
    @BeforeEach
    public void init() throws IOException {
        cacheTypeEnum = CacheTypeEnum.FILE;
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();
        discoveryServiceInterface = mock(DiscoveryServiceInterface.class);
        treeQueryClusterRunnerProxyInterface = mock(TreeQueryClusterRunnerProxyInterface.class);
        treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
    }

    @Test
    public void SimpleAsyncJoinTestWithSameCluster() throws Exception{
        String AvroTree = "SimpleJoinMongo.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        Node rootNode = JsonInstructionHelper.createNode(jsonString);
        assertThat(rootNode).isInstanceOf(JoinNode.class);
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
                            .discoveryServiceInterface(discoveryServiceInterface)
                            .build();
                })
                .build();
        treeQueryClusterService.runQueryTreeNetwork(rootNode, (status)->{
            log.debug(status.toString());
            synchronized (rootNode) {
                rootNode.notify();
            }

            assertThat(status.status).isEqualTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
            if(status.status!= StatusTreeQueryCluster.QueryTypeEnum.SUCCESS)
                throw new IllegalStateException(status.toString());
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
                    String isinBondTrade = GenericRecordSchemaHelper.StringifyAvroValue(record, "bondtrade.asset.securityId");
                    String isinSecCode = GenericRecordSchemaHelper.StringifyAvroValue(record,"bondstatic.isin_code");
                    assertEquals(isinBondTrade, isinSecCode);
                    assertThat(isinBondTrade.length()).isGreaterThan(5);
                });

        assertEquals(1000, counter.get());
    }
}
