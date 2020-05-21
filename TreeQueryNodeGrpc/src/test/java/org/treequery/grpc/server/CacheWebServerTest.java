package org.treequery.grpc.server;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.discoveryservicestatic.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.grpc.utils.TestDataAgent;
import org.treequery.grpc.utils.WebServerFactory;
import org.treequery.grpc.utils.proxy.GrpcCacheInputInterfaceProxyFactory;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.proxy.GrpcTreeQueryClusterRunnerProxy;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.AvroSchemaHelper;
import org.treequery.utils.BasicAvroSchemaHelperImpl;
import org.treequery.utils.TreeQuerySettingHelper;

public class CacheWebServerTest {
    static WebServer webServerA, webServerB;
    final static int PORT = 9002;//ThreadLocalRandom.current().nextInt(9000,9999);
    final static String HOSTNAME = "localhost";
    static TreeQuerySetting treeQuerySetting;
    static DiscoveryServiceInterface discoveryServiceInterface;
    static AvroSchemaHelper avroSchemaHelper;
    static CacheInputInterface cacheInputInterface;
    static TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;
    static TreeQueryRequest.RunMode RUNMODE = TreeQueryRequest.RunMode.DIRECT;
    static boolean RENEW_CACHE = false;

    @BeforeAll
    static void init() throws Exception{
        String AvroTree = "SimpleJoin.json";
        treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
        discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();

        discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();

        discoveryServiceInterface.registerCluster(
                treeQuerySetting.getCluster(),
                treeQuerySetting.getServicehostname(), treeQuerySetting.getServicePort());


        cacheInputInterface = prepareCacheInputInterface(treeQuerySetting, discoveryServiceInterface);

        treeQueryClusterRunnerProxyInterface = createRemoteProxy();//createLocalRunProxy();//
        webServerA = WebServerFactory.createWebServer(
                treeQuerySetting,
                discoveryServiceInterface,
                treeQueryClusterRunnerProxyInterface
        );
        webServerA.start();

    }

    @Test
    void queryNonExistIdentifier_throwException(){
        String identifer = "ABCD";
        String avroSchema = null;



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
