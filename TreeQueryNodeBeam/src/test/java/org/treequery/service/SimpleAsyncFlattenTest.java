package org.treequery.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.treequery.Transform.FlattenNode;
import org.treequery.beam.cache.BeamCacheOutputBuilder;
import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;
import org.treequery.discoveryservice.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.model.Node;
import org.treequery.service.proxy.LocalDummyTreeQueryClusterRunnerProxy;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.*;
import org.treequery.utils.proxy.CacheInputInterfaceProxyFactory;
import org.treequery.utils.proxy.LocalCacheInputInterfaceProxyFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
@Slf4j
public class SimpleAsyncFlattenTest {
    TreeQueryClusterService treeQueryClusterService = null;
    AvroSchemaHelper avroSchemaHelper = null;
    static DiscoveryServiceInterface discoveryServiceInterface = null;
    TreeQuerySetting treeQuerySetting = null;
    final static int PORT = 9002;//ThreadLocalRandom.current().nextInt(9000,9999);
    final static String HOSTNAME = "localhost";
    TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;
    CacheInputInterface cacheInputInterface;
    static AtomicLong NumberOfTradeSamples = new AtomicLong(0);

    @BeforeAll
    public static void staticinit(){
//        discoveryServiceInterface = new DiscoveryServiceProxyImpl();
        discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        List<String> fileList = Arrays.asList(new String[]{"bondtrade1.avro","bondtrade2.avro","bondtrade3.avro"});
        fileList.forEach(
                fileName-> {
                    File file = getWorkDirectory(fileName);
                    try {
                        AvroIOHelper.readAvroGenericRecordFile(
                                file, null,
                                (genericRecord -> {
                                    NumberOfTradeSamples.incrementAndGet();
                                })
                        );
                    }catch(Exception ex){
                        ex.printStackTrace();
                    }
                }
        );
        log.debug(String.format("Number of records %d", NumberOfTradeSamples.get()));

    }
    static File getWorkDirectory(String jsonFileName){
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        return new File(classLoader.getResource(jsonFileName).getFile());
    }
    @BeforeEach
    public void init() throws IOException {
        treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();
        Cluster clusterA = Cluster.builder().clusterName("A").build();
        Cluster clusterB = Cluster.builder().clusterName("B").build();
        discoveryServiceInterface.registerCluster(clusterA, HOSTNAME, PORT);
        discoveryServiceInterface.registerCluster(clusterB, HOSTNAME, PORT);

        CacheInputInterfaceProxyFactory cacheInputInterfaceProxyFactory = new LocalCacheInputInterfaceProxyFactory();
        cacheInputInterface = cacheInputInterfaceProxyFactory.getDefaultCacheInterface(treeQuerySetting, discoveryServiceInterface);

        treeQueryClusterRunnerProxyInterface = createLocalRunProxy();
    }

    @RepeatedTest(1)
    public void SimpleAsyncJoinTestWithSameCluster() throws Exception{
        String AvroTree = "TreeQueryInputFlattenOnly.json";
        this.runTest(AvroTree);
    }

    @RepeatedTest(1)
    public void SimpleAsyncJoinTestWithDiffCluster() throws Exception{
        String AvroTree = "TreeQueryInputFlattenClusterOnly.json";
        this.runTest(AvroTree);
    }
    @RepeatedTest(1)
    public void SimpleAsyncJoinTestWithDiffCluster2() throws Exception{
        String AvroTree = "TreeQueryInputFlattenClusterOnly2.json";
        this.runTest(AvroTree);
    }

    @RepeatedTest(2)
    public void SimpleAsyncJoinTest2layers() throws Exception{
        String AvroTree = "TreeQueryInput3.new.json";
        this.runTest(AvroTree);
    }

    private void runTest(String AvroTree) throws Exception{
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        Node rootNode = JsonInstructionHelper.createNode(jsonString);
        assertThat(rootNode).isInstanceOf(FlattenNode.class);
        log.debug("Run for data Identifier:"+ rootNode.getIdentifier());
        treeQueryClusterService =  AsyncTreeQueryClusterService.builder()
                .treeQueryClusterRunnerFactory(()->{
                    return TreeQueryClusterRunnerImpl.builder()
                            .beamCacheOutputBuilder(BeamCacheOutputBuilder.builder()
                                    .treeQuerySetting(this.treeQuerySetting)
                                    .build())
                            .avroSchemaHelper(avroSchemaHelper)
                            .treeQuerySetting(treeQuerySetting)
                            .treeQueryClusterRunnerProxyInterface(treeQueryClusterRunnerProxyInterface)
                            .cacheInputInterface(cacheInputInterface)
                            .discoveryServiceInterface(discoveryServiceInterface)
                            .build();
                })
                .build();
        final AsyncRunHelper asyncRunHelper =  AsyncRunHelper.create();
        treeQueryClusterService.runQueryTreeNetwork(rootNode, (status)->{
            log.debug(status.toString());

            boolean IsIssue = status.status!= StatusTreeQueryCluster.QueryTypeEnum.SUCCESS;

            if (IsIssue || status.getNode().getIdentifier().equals(rootNode.getIdentifier()))
                asyncRunHelper.continueRun(status);

            if(IsIssue)
                throw new IllegalStateException(status.toString());

        });
        StatusTreeQueryCluster statusTreeQueryCluster = asyncRunHelper.waitFor();
        if (statusTreeQueryCluster.getStatus() != StatusTreeQueryCluster.QueryTypeEnum.SUCCESS){
            throw new RuntimeException(statusTreeQueryCluster.getDescription());
        }

        log.debug("Retrieve result now");
        //Check the avro file
        String identifier = rootNode.getIdentifier();
        log.debug("Look for data Identifier:"+identifier+"from:"+discoveryServiceInterface.toString());
        Cluster getCluster = Optional.ofNullable(discoveryServiceInterface.getCacheResultCluster(identifier))
                .orElseThrow(()->{
                    return new RuntimeException("No cluster found for "+identifier+" map: "+discoveryServiceInterface.toString());
                });

        assertThat(getCluster)
                .isEqualTo(rootNode.getCluster());


        long pageSize = 10000;
        long page = 1;
        AtomicInteger counter = new AtomicInteger();
        Set<GenericRecord> genericRecordSet = Sets.newHashSet();
        Schema schema = AvroIOHelper.getPageRecordFromAvroCache(
                treeQuerySetting,
                rootNode.getIdentifier(),pageSize,page,
                (record)->{
                    assertThat(record).isNotNull();
                    counter.incrementAndGet();
                    genericRecordSet.add(record);
                });

        assertEquals(NumberOfTradeSamples.get(), counter.get());
        assertThat(genericRecordSet).hasSize((int)NumberOfTradeSamples.get());
    }

    private  TreeQueryClusterRunnerProxyInterface createLocalRunProxy(){
        return LocalDummyTreeQueryClusterRunnerProxy.builder()
                .treeQuerySetting(treeQuerySetting)
                .avroSchemaHelper(avroSchemaHelper)
                .createLocalTreeQueryClusterRunnerFunc(
                        (_Cluster)-> {

                            TreeQuerySetting remoteDummyTreeQuerySetting = new TreeQuerySetting(
                                    _Cluster.getClusterName(),
                                    treeQuerySetting.getServicehostname(),
                                    treeQuerySetting.getServicePort(),
                                    treeQuerySetting.getCacheFilePath(),
                                    treeQuerySetting.getRedisHostName(),
                                    treeQuerySetting.getRedisPort()
                            );
                            return TreeQueryClusterRunnerImpl.builder()
                                    .beamCacheOutputBuilder(BeamCacheOutputBuilder.builder()
                                            .treeQuerySetting(treeQuerySetting)
                                            .build())
                                    .avroSchemaHelper(avroSchemaHelper)
                                    .treeQuerySetting(remoteDummyTreeQuerySetting)
                                    .cacheInputInterface(cacheInputInterface)
                                    .discoveryServiceInterface(discoveryServiceInterface)
                                    .build();
                        }
                )
                .build();
    }

}
