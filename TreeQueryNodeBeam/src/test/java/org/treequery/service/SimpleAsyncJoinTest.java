package org.treequery.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.Transform.JoinNode;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.utils.AvroSchemaHelper;
import org.treequery.model.BasicAvroSchemaHelperImpl;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.utils.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

@Slf4j
public class SimpleAsyncJoinTest {
    TreeQueryClusterService treeQueryClusterService = null;

    BeamCacheOutputInterface beamCacheOutputInterface = null;
    CacheTypeEnum cacheTypeEnum;
    AvroSchemaHelper avroSchemaHelper = null;
    DiscoveryServiceInterface discoveryServiceInterface = null;

    @BeforeEach
    public void init() throws IOException {
        cacheTypeEnum = CacheTypeEnum.FILE;
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();
        beamCacheOutputInterface = new TestFileBeamCacheOutputImpl();
        discoveryServiceInterface = mock(DiscoveryServiceInterface.class);
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

    @Test
    public void FaultSimpleAsyncJoinTestWithSameCluster() throws Exception{
        String AvroTree = "SimpleJoinFault.json";
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

        });
        StatusTreeQueryCluster statusTreeQueryCluster = asyncRunHelper.waitFor();

        assertThat(statusTreeQueryCluster.getStatus()).isNotEqualTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
        log.debug(statusTreeQueryCluster.getDescription());



    }
}
