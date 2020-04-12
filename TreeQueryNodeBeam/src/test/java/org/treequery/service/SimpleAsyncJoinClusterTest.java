package org.treequery.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.Transform.JoinNode;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.model.BasicAvroSchemaHelperImpl;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.utils.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class SimpleAsyncJoinClusterTest {
    TreeQueryClusterService treeQueryClusterService = null;

    BeamCacheOutputInterface beamCacheOutputInterface = null;
    CacheTypeEnum cacheTypeEnum;
    AvroSchemaHelper avroSchemaHelper = null;
    DiscoveryServiceInterface discoveryServiceInterface = null;

    TreeQuerySetting treeQuerySetting;
    final static int PORT = 9002;//ThreadLocalRandom.current().nextInt(9000,9999);
    final static String HOSTNAME = "localhost";

    Cluster myCluster;

    @BeforeEach
    public void init() throws IOException {
        cacheTypeEnum = CacheTypeEnum.FILE;
        treeQuerySetting = SettingInitializer.createTreeQuerySetting();
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();
        beamCacheOutputInterface = new TestFileBeamCacheOutputImpl();
        discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        Cluster clusterA = Cluster.builder().clusterName("A").build();
        Cluster clusterB = Cluster.builder().clusterName("B").build();
        discoveryServiceInterface.registerCluster(clusterA, HOSTNAME, PORT);
        discoveryServiceInterface.registerCluster(clusterB, HOSTNAME, PORT);
        myCluster = Cluster.builder().clusterName(treeQuerySetting.getCluster()).build();
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
                            .discoveryServiceInterface(discoveryServiceInterface)
                            .build();
                })
                .build();
        final AsyncRunHelper asyncRunHelper =  AsyncRunHelper.of(rootNode);
        treeQueryClusterService.runQueryTreeNetwork(rootNode, (status)->{
            log.debug(status.toString());
            asyncRunHelper.continueRun(status);

            assertThat(status.status).isEqualTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
            if(status.status!= StatusTreeQueryCluster.QueryTypeEnum.SUCCESS)
                throw new IllegalStateException(status.toString());
        });
        StatusTreeQueryCluster statusTreeQueryCluster = asyncRunHelper.waitFor();
        if (statusTreeQueryCluster.getStatus() != StatusTreeQueryCluster.QueryTypeEnum.SUCCESS){
            throw new RuntimeException(statusTreeQueryCluster.getDescription());
        }else{
            discoveryServiceInterface.registerCacheResult(rootNode.getIdentifier(), myCluster);
        }

        //Check the avro file
        TestFileBeamCacheOutputImpl testFileBeamCacheOutput = (TestFileBeamCacheOutputImpl) beamCacheOutputInterface;
        File avroOutputFile = testFileBeamCacheOutput.getFile();
        AtomicInteger counter = new AtomicInteger();
        AvroIOHelper.readAvroGenericRecordFile(avroOutputFile,avroSchemaHelper.getAvroSchema(rootNode),
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
