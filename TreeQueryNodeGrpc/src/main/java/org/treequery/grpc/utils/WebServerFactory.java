package org.treequery.grpc.utils;

import io.grpc.BindableService;
import org.treequery.beam.cache.BeamCacheOutputBuilder;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.grpc.controller.SyncHealthCheckGrpcController;
import org.treequery.grpc.controller.SyncTreeQueryGrpcController;
import org.treequery.grpc.server.WebServer;
import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.model.CacheTypeEnum;
import org.treequery.service.TreeQueryClusterRunnerImpl;
import org.treequery.service.proxy.LocalDummyTreeQueryClusterRunnerProxy;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.AvroSchemaHelper;
import org.treequery.utils.BasicAvroSchemaHelperImpl;

import java.util.Arrays;

public class WebServerFactory {

    static TreeQueryBeamServiceHelper treeQueryBeamServiceHelper;
    static DiscoveryServiceInterface discoveryServiceInterface;
    static AvroSchemaHelper avroSchemaHelper;
    static TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;

    public static WebServer createLocalDummyWebServer(TreeQuerySetting treeQuerySetting){
        CacheTypeEnum cacheTypeEnum = CacheTypeEnum.FILE;

        avroSchemaHelper = new BasicAvroSchemaHelperImpl();
        discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();

        treeQueryClusterRunnerProxyInterface = LocalDummyTreeQueryClusterRunnerProxy.builder()
                .treeQuerySetting(treeQuerySetting)
                .cacheTypeEnum(cacheTypeEnum)
                .avroSchemaHelper(avroSchemaHelper)
                .createLocalTreeQueryClusterRunnerFunc(
                        (_Cluster)-> TreeQueryClusterRunnerImpl.builder()
                                .beamCacheOutputBuilder(BeamCacheOutputBuilder.builder()
                                        .cacheTypeEnum(cacheTypeEnum)
                                        .treeQuerySetting(treeQuerySetting)
                                        .build())
                                .cacheTypeEnum(cacheTypeEnum)
                                .avroSchemaHelper(avroSchemaHelper)
                                .treeQuerySetting(treeQuerySetting)
                                .discoveryServiceInterface(discoveryServiceInterface)
                                .build()
                )
                .build();
        treeQueryBeamServiceHelper =  TreeQueryBeamServiceHelper.builder()
                .cacheTypeEnum(CacheTypeEnum.FILE)
                .avroSchemaHelper(avroSchemaHelper)
                .discoveryServiceInterface(discoveryServiceInterface)
                .treeQuerySetting(treeQuerySetting)
                .treeQueryClusterRunnerProxyInterface(treeQueryClusterRunnerProxyInterface)
                .build();
        BindableService syncTreeQueryGrpcController = SyncTreeQueryGrpcController.builder()
                .treeQueryBeamServiceHelper(treeQueryBeamServiceHelper).build();

        BindableService[] bindableServices = {new SyncHealthCheckGrpcController(), syncTreeQueryGrpcController};

        WebServer webServer = new WebServer(treeQuerySetting.getServicePort(), Arrays.asList(bindableServices));
        return webServer;
    }
}