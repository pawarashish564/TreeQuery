package org.treequery.grpc.server;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.treequery.beam.cache.BeamCacheOutputBuilder;
import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.grpc.client.HealthWebClient;
import org.treequery.grpc.client.TreeQueryClient;
import org.treequery.grpc.exception.FailConnectionException;
import org.treequery.grpc.model.TreeQueryResult;
import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.grpc.utils.TestDataAgent;
import org.treequery.grpc.utils.WebServerFactory;
import org.treequery.grpc.utils.proxy.GrpcCacheInputInterfaceProxyFactory;
import org.treequery.service.TreeQueryClusterRunnerImpl;
import org.treequery.service.proxy.GrpcTreeQueryClusterRunnerProxy;
import org.treequery.service.proxy.LocalDummyTreeQueryClusterRunnerProxy;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.utils.AvroSchemaHelper;
import org.treequery.utils.BasicAvroSchemaHelperImpl;
import org.treequery.utils.TreeQuerySettingHelper;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

@Slf4j
class TreeQueryWebServerTest {
    static WebServer webServer;
    final static int PORT = 9002;//ThreadLocalRandom.current().nextInt(9000,9999);
    final static String HOSTNAME = "localhost";
    static String jsonString;
    static TreeQueryBeamServiceHelper treeQueryBeamServiceHelper;
    static DiscoveryServiceInterface discoveryServiceInterface;
    static AvroSchemaHelper avroSchemaHelper;
    static TreeQuerySetting treeQuerySetting;
    static TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;
    static CacheInputInterface cacheInputInterface;
    static TreeQueryRequest.RunMode RUNMODE = TreeQueryRequest.RunMode.DIRECT;
    static boolean RENEW_CACHE = false;

    @BeforeAll
    static void init() throws Exception{
        String AvroTree = "SimpleJoin.json";
        treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
        discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();
        jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        discoveryServiceInterface.registerCluster(
                Cluster.builder().clusterName("A").build(),
                HOSTNAME, PORT);
        discoveryServiceInterface.registerCluster(
                Cluster.builder().clusterName("B").build(),
                HOSTNAME, PORT);

        cacheInputInterface = prepareCacheInputInterface(treeQuerySetting, discoveryServiceInterface);

        treeQueryClusterRunnerProxyInterface = creatLocalRunProxy();
        webServer = WebServerFactory.createWebServer(
                treeQuerySetting,
                discoveryServiceInterface,
                treeQueryClusterRunnerProxyInterface);

        webServer.start();
        //webServer.blockUntilShutdown();
    }

    private static TreeQueryClusterRunnerProxyInterface createRemoteProxy(){
        return GrpcTreeQueryClusterRunnerProxy.builder()
                .discoveryServiceInterface(discoveryServiceInterface)
                .runMode(RUNMODE)
                .renewCache(RENEW_CACHE)
                .build();
    }

    private static TreeQueryClusterRunnerProxyInterface creatLocalRunProxy(){
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

    @Test
    void healthCheckClient() {
        HealthWebClient healthWebClient = new HealthWebClient(HOSTNAME, PORT);
        boolean checkStatus = healthWebClient.healthCheck();
        assertTrue(checkStatus);
        log.info(String.format("Web client health check %b", checkStatus));
    }

    @Test
    void failtoConnect(){
        String AvroTree = "SimpleJoin.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        TreeQueryClient treeQueryClient = new TreeQueryClient(HOSTNAME, PORT+20);

        boolean renewCache = false;
        int pageSize = 100;
        int page = 1;
        TreeQueryResult treeQueryResult = null;
        AtomicLong counter = new AtomicLong(0);
        Set<GenericRecord> genericRecordSet = Sets.newHashSet();
        assertThrows(FailConnectionException.class,()->{
             treeQueryClient.query(TreeQueryRequest.RunMode.DIRECT,
                    jsonString,
                    renewCache,
                    pageSize,
                    page
            );
        });

    }

    @Test
    void happyPathSimpleJoin(){
        String AvroTree = "SimpleJoin.json";
        run(AvroTree);
    }
    @Test
    void happyPathSimpleClusterJoin(){
        String AvroTree = "SimpleJoinCluster.json";
        run(AvroTree);
    }

    void run(String AvroTree){
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        TreeQueryClient treeQueryClient = new TreeQueryClient(HOSTNAME, PORT);

        boolean renewCache = false;
        int pageSize = 100;
        int page = 1;
        TreeQueryResult treeQueryResult = null;
        AtomicLong counter = new AtomicLong(0);
        Set<GenericRecord> genericRecordSet = Sets.newHashSet();
        do {
            treeQueryResult = treeQueryClient.query(TreeQueryRequest.RunMode.DIRECT,
                    jsonString,
                    renewCache,
                    pageSize,
                    page
            );
            assertTrue(treeQueryResult.getHeader().isSuccess());
            assertEquals(0, treeQueryResult.getHeader().getErr_code());
            TreeQueryResult.TreeQueryResponseResult treeQueryResponseResult = treeQueryResult.getResult();

            List<GenericRecord> genericRecordList = treeQueryResponseResult.getGenericRecordList();
            genericRecordList.forEach(
                    genericRecord -> {
                        assertThat(genericRecord).isNotNull();
                        assertThat(genericRecord.get("bondtrade")).isNotNull();
                        assertThat(genericRecordSet).doesNotContain(genericRecord);
                        counter.incrementAndGet();
                        genericRecordSet.add(genericRecord);
                    }
            );
            page++;
        }while(treeQueryResult!=null && treeQueryResult.getResult().getDatasize()!=0);
        assertEquals(1000, counter.get());
        assertThat(genericRecordSet).hasSize(1000);
    }

    private static CacheInputInterface prepareCacheInputInterface(TreeQuerySetting treeQuerySetting,
                                                                  DiscoveryServiceInterface discoveryServiceInterface){
        return new GrpcCacheInputInterfaceProxyFactory()
                .getDefaultCacheInterface(treeQuerySetting, discoveryServiceInterface);
    }

    @Test
    void testByteStream() throws Exception{
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte [] byteData = byteArrayOutputStream.toByteArray();
        assertNotNull(byteData);
        byteArrayOutputStream.close();
    }

    @AfterAll
    static void finish() throws Exception{
        log.info("All testing finish");
        webServer.stop();
    }
}