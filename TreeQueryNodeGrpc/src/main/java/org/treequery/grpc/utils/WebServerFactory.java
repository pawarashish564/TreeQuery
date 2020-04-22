package org.treequery.grpc.utils;

import io.grpc.BindableService;
import org.treequery.beam.cache.BeamCacheOutputBuilder;
import org.treequery.beam.cache.TreeQueryCacheProxy;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.grpc.controller.SyncHealthCheckGrpcController;
import org.treequery.grpc.controller.SyncTreeQueryCacheGrpcController;
import org.treequery.grpc.controller.SyncTreeQueryGrpcController;
import org.treequery.grpc.server.WebServer;
import org.treequery.grpc.service.TreeQueryBeamService;
import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.grpc.service.TreeQueryCacheService;
import org.treequery.grpc.service.TreeQueryCacheServiceHelper;
import org.treequery.grpc.utils.proxy.GrpcCacheInputInterfaceProxyFactory;
import org.treequery.model.CacheTypeEnum;
import org.treequery.service.TreeQueryClusterRunnerImpl;
import org.treequery.service.proxy.LocalDummyTreeQueryClusterRunnerProxy;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.AvroSchemaHelper;
import org.treequery.utils.BasicAvroSchemaHelperImpl;
import org.treequery.utils.TreeQuerySettingHelper;
import org.treequery.utils.proxy.LocalCacheInputInterfaceProxyFactory;
import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.utils.proxy.CacheInputInterfaceProxyFactory;

import java.util.Arrays;

public class WebServerFactory {

    static TreeQueryBeamService treeQueryBeamService;
    static AvroSchemaHelper avroSchemaHelper;
    static TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;
    static CacheInputInterface cacheInputInterface;

    public static WebServer createWebServer(TreeQuerySetting treeQuerySetting,
                                            DiscoveryServiceInterface discoveryServiceInterface){
        CacheTypeEnum cacheTypeEnum = treeQuerySetting.getCacheTypeEnum();
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();
        cacheInputInterface = prepareCacheInputInterface(treeQuerySetting, discoveryServiceInterface);


        treeQueryClusterRunnerProxyInterface = LocalDummyTreeQueryClusterRunnerProxy.builder()
                .treeQuerySetting(treeQuerySetting)
                .cacheTypeEnum(cacheTypeEnum)
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
                                            .cacheTypeEnum(cacheTypeEnum)
                                            .treeQuerySetting(treeQuerySetting)
                                            .build())
                                    .cacheTypeEnum(cacheTypeEnum)
                                    .avroSchemaHelper(avroSchemaHelper)
                                    .treeQuerySetting(remoteDummyTreeQuerySetting)
                                    .cacheInputInterface(cacheInputInterface)
                                    .discoveryServiceInterface(discoveryServiceInterface)
                                    .build();
                        }
                )
                .build();
        treeQueryBeamService =  TreeQueryBeamServiceHelper.builder()
                .cacheTypeEnum(CacheTypeEnum.FILE)
                .avroSchemaHelper(avroSchemaHelper)
                .discoveryServiceInterface(discoveryServiceInterface)
                .treeQuerySetting(treeQuerySetting)
                .treeQueryClusterRunnerProxyInterface(treeQueryClusterRunnerProxyInterface)
                .cacheInputInterface(cacheInputInterface)
                .build();
        BindableService syncTreeQueryGrpcController = SyncTreeQueryGrpcController.builder()
                .treeQueryBeamService(treeQueryBeamService).build();
        BindableService syncTreeQueryCacheGrpcController = prepareCacheController(treeQuerySetting);

        BindableService[] bindableServices = {new SyncHealthCheckGrpcController(),
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
        return SyncTreeQueryCacheGrpcController.builder()
                .treeQueryCacheService(treeQueryCacheService)
                .build();
    }
}
