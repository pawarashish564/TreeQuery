package org.treequery.grpc.utils;

import io.grpc.BindableService;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.grpc.controller.HealthCheckGrpcController;
import org.treequery.grpc.controller.TreeQueryCacheGrpcController;
import org.treequery.grpc.controller.TreeQueryGrpcController;
import org.treequery.grpc.server.WebServer;
import org.treequery.grpc.service.TreeQueryBeamService;
import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.grpc.service.TreeQueryCacheService;
import org.treequery.grpc.service.TreeQueryCacheServiceHelper;
import org.treequery.grpc.utils.proxy.GrpcCacheInputInterfaceProxyFactory;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.AvroSchemaHelper;
import org.treequery.utils.BasicAvroSchemaHelperImpl;
import org.treequery.beam.cache.CacheInputInterface;

import java.util.Arrays;

public class WebServerFactory {

    static TreeQueryBeamService treeQueryBeamService;
    static AvroSchemaHelper avroSchemaHelper;
    static CacheInputInterface cacheInputInterface;

    public static WebServer createWebServer(TreeQuerySetting treeQuerySetting,
                                            DiscoveryServiceInterface discoveryServiceInterface,
                                            TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface){
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();
        cacheInputInterface = prepareCacheInputInterface(treeQuerySetting, discoveryServiceInterface);



        treeQueryBeamService =  TreeQueryBeamServiceHelper.builder()
                .avroSchemaHelper(avroSchemaHelper)
                .discoveryServiceInterface(discoveryServiceInterface)
                .treeQuerySetting(treeQuerySetting)
                .treeQueryClusterRunnerProxyInterface(treeQueryClusterRunnerProxyInterface)
                .cacheInputInterface(cacheInputInterface)
                .build();
        BindableService syncTreeQueryGrpcController = TreeQueryGrpcController.builder()
                .treeQueryBeamService(treeQueryBeamService).build();
        BindableService syncTreeQueryCacheGrpcController = prepareCacheController(treeQuerySetting);

        BindableService[] bindableServices = {new HealthCheckGrpcController(),
                syncTreeQueryGrpcController,
                syncTreeQueryCacheGrpcController};

        WebServer webServer = new WebServer(treeQuerySetting.getServicePort(), Arrays.asList(bindableServices));
        return webServer;
    }

    private static CacheInputInterface prepareCacheInputInterface(TreeQuerySetting treeQuerySetting,
                                                                  DiscoveryServiceInterface discoveryServiceInterface){
        return new GrpcCacheInputInterfaceProxyFactory()
                .getDefaultCacheInterface(treeQuerySetting, discoveryServiceInterface);
    }

    private static BindableService prepareCacheController(TreeQuerySetting treeQuerySetting){
        TreeQueryCacheService treeQueryCacheService = TreeQueryCacheServiceHelper.builder()
                .treeQuerySetting(treeQuerySetting)
                .build();
        return TreeQueryCacheGrpcController.builder()
                .treeQueryCacheService(treeQueryCacheService)
                .build();
    }
}
