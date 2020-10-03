package org.treequery.service;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.data.Offset;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.junit.jupiter.MockitoExtension;
import org.treequery.beam.cache.BeamCacheOutputBuilder;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.discoveryservicestatic.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.*;
import org.treequery.utils.EventBus.BasicTreeNodeEventBusTrafficLightImpl;
import org.treequery.utils.EventBus.TreeNodeEventBusTrafficLight;

import org.treequery.model.Node;
import org.treequery.utils.proxy.CacheInputInterfaceProxyFactory;
import org.treequery.utils.proxy.LocalCacheInputInterfaceProxyFactory;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@ExtendWith(MockitoExtension.class)
class BatchTreeQueryClusterServiceEventImplTest {
    private static TreeQuerySetting treeQuerySetting;
    private static TreeQuerySetting treeQuerySettingB;
    private static int WAITSECOND = 60 ;
    private final static int PORT = 9002;//ThreadLocalRandom.current().nextInt(9000,9999);
    private final static String HOSTNAME = "localhost";
    static DiscoveryServiceInterface discoveryServiceInterface = null;
    Node node;
    TreeNodeEventBusTrafficLight<Node> eventTreeNodeEventBusTrafficLight;
    TreeQueryClusterRunner localTreeQueryClusterRunner;
    TreeQueryClusterRunner remoteTreeQueryClusterRunner;

    AvroSchemaHelper avroSchemaHelper;
    CacheInputInterface cacheInputInterface;

    @BeforeAll
    static void initAll(){
        treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
        treeQuerySettingB = TreeQuerySettingHelper.createFromYaml("treeQueryB.yaml",false);
        discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        Cluster clusterA = Cluster.builder().clusterName("A").build();
        Cluster clusterB = Cluster.builder().clusterName("B").build();
        discoveryServiceInterface.registerCluster(clusterA, HOSTNAME, PORT);
        discoveryServiceInterface.registerCluster(clusterB, HOSTNAME, PORT);
    }

    static Node prepareSample(String AvroTree) throws Exception{
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        Node node = JsonInstructionHelper.createNode(jsonString);
        return node;
    }

    @BeforeEach
    void initTreeNode() throws Exception{
        String AvroTree = "TreeQueryInput4.json";
        node = prepareSample(AvroTree);

        eventTreeNodeEventBusTrafficLight = new BasicTreeNodeEventBusTrafficLightImpl();
        localTreeQueryClusterRunner = new DummyLocalTreeQueryClusterRunner(
                treeQuerySetting, StatusTreeQueryCluster.QueryTypeEnum.SUCCESS
        );
        remoteTreeQueryClusterRunner = new DummyRemoteTreeQueryClusterRunner(
            treeQuerySetting, StatusTreeQueryCluster.QueryTypeEnum.SUCCESS
        );
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();

        CacheInputInterfaceProxyFactory cacheInputInterfaceProxyFactory = new LocalCacheInputInterfaceProxyFactory();
        cacheInputInterface = cacheInputInterfaceProxyFactory.getDefaultCacheInterface(treeQuerySetting, discoveryServiceInterface);


    }

    @Test
    void shouldRunSimpleJoinLocalRunner() throws Exception{
        String AvroTree = "SimpleJoin.json";
        node = prepareSample(AvroTree);
        runQuery(node);
        checkSimpleJoinCriteria(node);
    }
    @Test
    void shouldRunSimpleClusterJoinLocalRunner() throws Exception{
        String AvroTree = "SimpleJoinCluster.json";
        node = prepareSample(AvroTree);

        remoteTreeQueryClusterRunner =  LocalTreeQueryClusterRunner.builder()
                .avroSchemaHelper(avroSchemaHelper)
                .beamCacheOutputBuilder(BeamCacheOutputBuilder.builder()
                        .treeQuerySetting(treeQuerySetting)
                        .build())
                .discoveryServiceInterface(discoveryServiceInterface)
                .treeQuerySetting(treeQuerySetting)
                .cacheInputInterface(cacheInputInterface)
                .build();

        runQuery(node);
        checkSimpleJoinCriteria(node);
    }

    @Test
    public void AsyncJoinTest4layers() throws Exception {
        String AvroTree = "TreeQueryInput4.json";
        node = prepareSample(AvroTree);
        remoteTreeQueryClusterRunner =  LocalTreeQueryClusterRunner.builder()
                .avroSchemaHelper(avroSchemaHelper)
                .beamCacheOutputBuilder(BeamCacheOutputBuilder.builder()
                        .treeQuerySetting(treeQuerySetting)
                        .build())
                .discoveryServiceInterface(discoveryServiceInterface)
                .treeQuerySetting(treeQuerySetting)
                .cacheInputInterface(cacheInputInterface)
                .build();
        runQuery(node);

        long pageSize = 10000;
        long page = 1;
        AtomicInteger counter = new AtomicInteger();
        Set<GenericRecord> genericRecordSet = Sets.newHashSet();
        try {
            Schema schema = AvroIOHelper.getPageRecordFromAvroCache(
                    treeQuerySetting,
                    node.getIdentifier(), pageSize, page,
                    (record) -> {
                        assertThat(record).isNotNull();
                        String bondTradeTenor = GenericRecordSchemaHelper.StringifyAvroValue(record, "bondtrade_bondstatic.bondstatic.original_maturity");
                        String bondMarketDataTenor = GenericRecordSchemaHelper.StringifyAvroValue(record, "bondprice.Tenor");
                        assertEquals(bondTradeTenor, bondMarketDataTenor);
                        GenericRecordSchemaHelper.DoubleField doubleField = new GenericRecordSchemaHelper.DoubleField();
                        GenericRecordSchemaHelper.getValue(record, "bondprice.Price", doubleField);
                        double refPrice=0;
                        if (bondTradeTenor.equals("10Y")){
                            refPrice = 0.72;
                        }else if(bondMarketDataTenor.equals("15Y")){
                            refPrice = 0.78;
                        }else if(bondMarketDataTenor.equals("5Y")){
                            refPrice = 0.6;
                        }else if(bondMarketDataTenor.equals("3Y")){
                            refPrice = 0.62;
                        }
                        assertThat(doubleField.getValue()).isCloseTo(refPrice, Offset.offset(0.0001));
                        counter.incrementAndGet();
                        genericRecordSet.add(record);

                    });
        }catch (CacheNotFoundException che){
            che.printStackTrace();
            throw new IllegalStateException(che.getMessage());
        }
        assertEquals(3000, genericRecordSet.size());
        assertEquals(3000, counter.get());
    }
    private void checkSimpleJoinCriteria(Node node){
        log.debug("Retrieve result now");
        //Check the avro file
        String identifier = node.getIdentifier();
        log.debug("Look for data Identifier:"+identifier+"from:"+discoveryServiceInterface.toString());
        Cluster getCluster = Optional.ofNullable(discoveryServiceInterface.getCacheResultCluster(identifier))
                .orElseThrow(()->{
                    return new RuntimeException("No cluster found for "+identifier+" map: "+discoveryServiceInterface.toString());
                });

        assertThat(getCluster)
                .isEqualTo(node.getCluster());

        long pageSize = 10000;
        long page = 1;
        AtomicInteger counter = new AtomicInteger();
        Set<GenericRecord> genericRecordSet = Sets.newHashSet();
        try {
            Schema schema = AvroIOHelper.getPageRecordFromAvroCache(
                    treeQuerySetting,
                    node.getIdentifier(), pageSize, page,
                    (record) -> {
                        assertThat(record).isNotNull();
                        counter.incrementAndGet();
                        String isinBondTrade = GenericRecordSchemaHelper.StringifyAvroValue(record, "bondtrade.asset.securityId");
                        String isinSecCode = GenericRecordSchemaHelper.StringifyAvroValue(record, "bondstatic.isin_code");
                        assertThat(genericRecordSet).doesNotContain(record);
                        assertEquals(isinBondTrade, isinSecCode);
                        assertThat(isinBondTrade.length()).isGreaterThan(5);
                        genericRecordSet.add(record);
                    });
        }catch (CacheNotFoundException che){
            che.printStackTrace();
            throw new IllegalStateException(che.getMessage());
        }
        assertEquals(1000, genericRecordSet.size());
        assertEquals(1000, counter.get());
    }

    @SneakyThrows
    private void runQuery(Node node){
        localTreeQueryClusterRunner =  LocalTreeQueryClusterRunner.builder()
                .avroSchemaHelper(avroSchemaHelper)
                .beamCacheOutputBuilder(BeamCacheOutputBuilder.builder()
                        .treeQuerySetting(treeQuerySetting)
                        .build())
                .discoveryServiceInterface(discoveryServiceInterface)
                .treeQuerySetting(treeQuerySetting)
                .cacheInputInterface(cacheInputInterface)
                .build();
        TreeQueryClusterService batchTreeQueryClusterServiceEvent = BatchTreeQueryClusterServiceEventImpl.builder()
                .localTreeQueryClusterRunner(localTreeQueryClusterRunner)
                .remoteTreeQueryClusterRunner(remoteTreeQueryClusterRunner)
                .treeQuerySetting(treeQuerySetting)
                .eventTreeNodeEventBusTrafficLight(eventTreeNodeEventBusTrafficLight)
                .discoveryServiceInterface(discoveryServiceInterface)
                .build();
        final AsyncRunHelper asyncRunHelper =  AsyncRunHelper.create();

        batchTreeQueryClusterServiceEvent.runQueryTreeNetwork(
                node,
                statusTreeQueryCluster -> {
                    try {
                        assertThat(statusTreeQueryCluster.getStatus()).isEqualByComparingTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
                    }finally {
                        asyncRunHelper.continueRun(statusTreeQueryCluster);
                    }
                }
        );
        StatusTreeQueryCluster statusTreeQueryCluster = asyncRunHelper.waitFor();
        assertThat(statusTreeQueryCluster.getStatus()).isEqualByComparingTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);

    }

    @Test
    void shouldRunDummyRuner() throws Exception{

        TreeQueryClusterService batchTreeQueryClusterServiceEvent = BatchTreeQueryClusterServiceEventImpl.builder()
                .localTreeQueryClusterRunner(localTreeQueryClusterRunner)
                .remoteTreeQueryClusterRunner(remoteTreeQueryClusterRunner)
                .treeQuerySetting(treeQuerySetting)
                .eventTreeNodeEventBusTrafficLight(eventTreeNodeEventBusTrafficLight)
                .discoveryServiceInterface(discoveryServiceInterface)
                .build();
        CountDownLatch countDownLatch = new CountDownLatch(1);

        final AsyncRunHelper asyncRunHelper =  AsyncRunHelper.create();
        batchTreeQueryClusterServiceEvent.runQueryTreeNetwork(
                node,
                statusTreeQueryCluster -> {
                    try {
                        assertThat(statusTreeQueryCluster.getStatus()).isEqualByComparingTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
                        asyncRunHelper.continueRun(statusTreeQueryCluster);
                    }finally {
                        countDownLatch.countDown();
                    }
                }
        );

        countDownLatch.await(WAITSECOND, TimeUnit.SECONDS);
        StatusTreeQueryCluster statusTreeQueryCluster = asyncRunHelper.waitFor();
        assertThat(statusTreeQueryCluster.getStatus()).isEqualByComparingTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);

    }

    @Test
    void shouldThrowException() throws Exception{

        localTreeQueryClusterRunner = new DummyLocalTreeQueryClusterRunner(
                treeQuerySetting, StatusTreeQueryCluster.QueryTypeEnum.FAIL
        );
        remoteTreeQueryClusterRunner = new DummyRemoteTreeQueryClusterRunner(
                treeQuerySetting, StatusTreeQueryCluster.QueryTypeEnum.FAIL
        );
        TreeQueryClusterService batchTreeQueryClusterServiceEvent = BatchTreeQueryClusterServiceEventImpl.builder()
                .localTreeQueryClusterRunner(localTreeQueryClusterRunner)
                .remoteTreeQueryClusterRunner(remoteTreeQueryClusterRunner)
                .treeQuerySetting(treeQuerySetting)
                .eventTreeNodeEventBusTrafficLight(eventTreeNodeEventBusTrafficLight)
                .discoveryServiceInterface(discoveryServiceInterface)
                .build();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        batchTreeQueryClusterServiceEvent.runQueryTreeNetwork(
                node,
                statusTreeQueryCluster -> {
                    log.debug("Get failure message:"+statusTreeQueryCluster.toString());
                    assertThat(statusTreeQueryCluster.getStatus()).isEqualByComparingTo(StatusTreeQueryCluster.QueryTypeEnum.FAIL);
                    countDownLatch.countDown();
                }
        );
        countDownLatch.await(WAITSECOND, TimeUnit.SECONDS);

    }

    @Slf4j
    @RequiredArgsConstructor
    static class DummyLocalTreeQueryClusterRunner implements TreeQueryClusterRunner{
        final TreeQuerySetting treeQuerySetting;
        final StatusTreeQueryCluster.QueryTypeEnum queryTypeEnum;
        @Override
        public void runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> StatusCallback) {
            log.debug("Running Local node:"+node.toJson());
            StatusTreeQueryCluster statusTreeQueryCluster = StatusTreeQueryCluster.builder()
                    .cluster(treeQuerySetting.getCluster())
                    .description(node.getDescription())
                    .node(node)
                    .status(queryTypeEnum)
                    .build();
            StatusCallback.accept(statusTreeQueryCluster);
        }

        @Override
        public String toString() {
            return "LocalCluster";
        }

        @Override
        public void setTreeQueryClusterRunnerProxyInterface(TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface) {
            throw new NoSuchMethodError();
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    static class DummyRemoteTreeQueryClusterRunner implements TreeQueryClusterRunner{
        final TreeQuerySetting treeQuerySetting;
        final StatusTreeQueryCluster.QueryTypeEnum queryTypeEnum;
        @Override
        public void runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> StatusCallback) {
            log.debug("Running Remote node:"+node.toJson());
            StatusTreeQueryCluster statusTreeQueryCluster = StatusTreeQueryCluster.builder()
                    .cluster(treeQuerySetting.getCluster())
                    .description(node.getDescription())
                    .node(node)
                    .status(queryTypeEnum)
                    .build();
            StatusCallback.accept(statusTreeQueryCluster);
        }

        @Override
        public String toString() {
            return "RemoteCluster";
        }

        @Override
        public void setTreeQueryClusterRunnerProxyInterface(TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface) {
            throw new NoSuchMethodError();
        }
    }

}