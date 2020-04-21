package org.treequery.beam.cache;

import io.grpc.BindableService;
import io.grpc.StatusRuntimeException;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.internal.matchers.Any;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.model.Location;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.grpc.controller.SyncHealthCheckGrpcController;
import org.treequery.grpc.controller.SyncTreeQueryCacheGrpcController;
import org.treequery.grpc.exception.FailConnectionException;
import org.treequery.grpc.server.WebServer;
import org.treequery.grpc.service.TreeQueryCacheService;
import org.treequery.grpc.service.TreeQueryCacheServiceHelper;
import org.treequery.grpc.utils.TestDataAgent;
import org.treequery.model.CacheTypeEnum;
import org.treequery.proto.TreeQueryCacheRequest;
import org.treequery.proto.TreeQueryCacheResponse;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TreeQueryCacheProxyTest {

    static TreeQueryCacheRequest treeQueryCacheRequest = null;
    static String identifier = "BondTradeJoinBondStatic";
    static String avroSampleFile = identifier+".avro";
    static TreeQuerySetting treeQuerySetting;
    static TreeQueryCacheService treeQueryCacheService;
    static SyncTreeQueryCacheGrpcController syncTreeQueryCacheGrpcController = null;
    static String HOSTNAME = "localhost";
    static int PORT = 9010;
    static WebServer webServer = null;
    static DiscoveryServiceInterface discoveryServiceInterface = null;

    @BeforeAll
    static void initAll() throws Exception{
        CacheTypeEnum cacheTypeEnum = CacheTypeEnum.FILE;
        TreeQuerySetting.TreeQuerySettingBuilder treeQuerySettingBuilder = new TreeQuerySetting.TreeQuerySettingBuilder(
                "A",
                HOSTNAME,
                PORT,
                TestDataAgent.getTestResourceDirectory(avroSampleFile),
                "",0
        );
        treeQuerySetting = treeQuerySettingBuilder.build();

        treeQueryCacheService = TreeQueryCacheServiceHelper.builder()
                .cacheTypeEnum(cacheTypeEnum)
                .treeQuerySetting(treeQuerySetting)
                .build();

        syncTreeQueryCacheGrpcController = SyncTreeQueryCacheGrpcController.builder()
                .treeQueryCacheService(treeQueryCacheService)
                .build();
        BindableService[] bindableServices = {new SyncHealthCheckGrpcController(),  syncTreeQueryCacheGrpcController};
        webServer = new WebServer(PORT, Arrays.asList(bindableServices));
        discoveryServiceInterface = mock(DiscoveryServiceInterface.class);
        webServer.start();
    }
    @Test
    void returnCacheNotFound() throws Exception{
        long page = 1;
        long pageSize = 100;
        Schema schema = null;
        CacheInputInterface cacheInputInterface = TreeQueryCacheProxy.builder()
                .discoveryServiceInterface(discoveryServiceInterface)
                .build();
        assertThrows( CacheNotFoundException.class,()->{
            cacheInputInterface.getPageRecordFromAvroCache(null,
                    CacheTypeEnum.NOTCARE,
                    identifier,
                    pageSize,
                    page, (record) -> {}, schema);
        });
        when(discoveryServiceInterface.getCacheResultCluster(any(String.class)))
                .thenReturn(Cluster.builder().clusterName("A").build());
        assertThrows( CacheNotFoundException.class,()->{
            cacheInputInterface.getPageRecordFromAvroCache(null,
                    CacheTypeEnum.NOTCARE,
                    identifier,
                    pageSize,
                    page, (record) -> {}, schema);
        });

        when(discoveryServiceInterface.getCacheResultCluster(any(String.class)))
                .thenReturn(Cluster.builder().clusterName("A").build());
        when(discoveryServiceInterface.getClusterLocation(any(Cluster.class)))
                .thenReturn(new Location(HOSTNAME, PORT+10));
        assertThrows( FailConnectionException.class,()->{
            cacheInputInterface.getPageRecordFromAvroCache(null,
                    CacheTypeEnum.NOTCARE,
                    identifier,
                    pageSize,
                    page, (record) -> {}, schema);
        });
    }

    @Test
    void happyPathgetPageRecordFromAvroCache()throws Exception {
        when(discoveryServiceInterface.getCacheResultCluster(any(String.class)))
                .thenReturn(Cluster.builder().clusterName("A").build());
        when(discoveryServiceInterface.getClusterLocation(any(Cluster.class)))
                .thenReturn(new Location(HOSTNAME, PORT));

        AtomicLong counter = new AtomicLong(0);
        long page = 1;
        long pageSize = 100;

        Schema schema = null;
        while (true) {
            long inx = counter.get();
            CacheInputInterface cacheInputInterface = TreeQueryCacheProxy.builder()
                    .discoveryServiceInterface(discoveryServiceInterface)
                    .build();
            schema = cacheInputInterface.getPageRecordFromAvroCache(null,
                    CacheTypeEnum.NOTCARE,
                    identifier,
                    pageSize,
                    page, (record) -> {
                        counter.incrementAndGet();
                    }, schema);
            page++;
            if (inx == counter.get()){
                break;
            }
        }
        assertEquals(1000, counter.get());
    }
    @AfterAll
    static void destroy(){
        webServer.stop();
    }
}