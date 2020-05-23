package org.treequery.grpc.server;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.beam.cache.StreamCacheProxy;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.discoveryservicestatic.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.grpc.exception.SchemaGetException;
import org.treequery.grpc.utils.TestDataAgent;
import org.treequery.grpc.utils.WebServerFactory;
import org.treequery.grpc.utils.proxy.GrpcCacheInputInterfaceProxyFactory;
import org.treequery.model.CacheTypeEnum;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.proxy.GrpcTreeQueryClusterRunnerProxy;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.AvroSchemaHelper;
import org.treequery.utils.BasicAvroSchemaHelperImpl;
import org.treequery.utils.TreeQuerySettingHelper;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
public class CacheWebServerTest {
    static WebServer webServer;
    final static int PORT = 9002;//ThreadLocalRandom.current().nextInt(9000,9999);
    final static String HOSTNAME = "localhost";
    static TreeQuerySetting treeQuerySetting;
    static DiscoveryServiceInterface discoveryServiceInterface;
    static AvroSchemaHelper avroSchemaHelper;
    static CacheInputInterface cacheInputInterface;
    static TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;
    static TreeQueryRequest.RunMode RUNMODE = TreeQueryRequest.RunMode.DIRECT;
    static boolean RENEW_CACHE = false;
    static String identifer = "BondTradeJoinBondStatic";

    @BeforeAll
    static void init() throws Exception{
        String avroSampleFile = String.format("%s.avro", identifer);
        treeQuerySetting = TestDataAgent
                .getTreeQuerySettingBackedByResources(HOSTNAME, PORT, avroSampleFile);
        discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();

        discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();

        discoveryServiceInterface.registerCluster(
                treeQuerySetting.getCluster(),
                treeQuerySetting.getServicehostname(), treeQuerySetting.getServicePort());


        cacheInputInterface = prepareCacheInputInterface(treeQuerySetting, discoveryServiceInterface);

        treeQueryClusterRunnerProxyInterface = createRemoteProxy();//createLocalRunProxy();//
        webServer = WebServerFactory.createWebServer(
                treeQuerySetting,
                discoveryServiceInterface,
                treeQueryClusterRunnerProxyInterface
        );
        webServer.start();

    }

    @Test
    void queryNonExistIdentifier_throwException(){
        String identifer = "ABCD";
        StreamCacheProxy streamCacheProxy = new StreamCacheProxy(discoveryServiceInterface);

        assertThrows(SchemaGetException.class, ()->{
            try {
                streamCacheProxy.getStreamRecordFromAvroCache(
                        treeQuerySetting.getCluster(),
                        identifer,
                        (record) -> {
                        },
                        null
                );
            }catch(Throwable th){
                log.error(th.toString());
                throw th;
            }
        });
    }
    @Test
    void queryExistIdentifier(){
        StreamCacheProxy streamCacheProxy = new StreamCacheProxy(discoveryServiceInterface);
        AtomicInteger countRecord = new AtomicInteger(0);
        streamCacheProxy.getStreamRecordFromAvroCache(
                treeQuerySetting.getCluster(),
                identifer,
                (record)->{
                    //log.debug(record.toString());
                    countRecord.incrementAndGet();
                },
                null
        );
        assertEquals(1000, countRecord.get());
    }


    @AfterAll
    static void finish() throws Exception{
        log.info("All testing finish");
        webServer.stop();
    }
    private static TreeQueryClusterRunnerProxyInterface createRemoteProxy(){
        return GrpcTreeQueryClusterRunnerProxy.builder()
                .discoveryServiceInterface(discoveryServiceInterface)
                .runMode(RUNMODE)
                .renewCache(RENEW_CACHE)
                .build();
    }
    private static CacheInputInterface prepareCacheInputInterface(TreeQuerySetting treeQuerySetting,
                                                                  DiscoveryServiceInterface discoveryServiceInterface){
        return new GrpcCacheInputInterfaceProxyFactory()
                .getDefaultCacheInterface(treeQuerySetting, discoveryServiceInterface);
    }
}
