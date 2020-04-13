package org.treequery.grpc.server;

import io.grpc.BindableService;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.grpc.controller.SyncHealthCheckGrpcController;
import org.treequery.grpc.controller.SyncTreeQueryGrpcController;
import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.service.proxy.LocalDummyTreeQueryClusterRunnerProxy;
import org.treequery.utils.BasicAvroSchemaHelperImpl;
import org.treequery.model.CacheTypeEnum;
import org.treequery.utils.AvroSchemaHelper;

import java.io.IOException;
import java.util.Arrays;


public class Main {
    static TreeQueryBeamServiceHelper treeQueryBeamServiceHelper;
    static DiscoveryServiceInterface discoveryServiceInterface;
    static AvroSchemaHelper avroSchemaHelper;
    static TreeQuerySetting treeQuerySetting;

    public static void main(String [] args) throws IOException, InterruptedException {
        CacheTypeEnum cacheTypeEnum = CacheTypeEnum.FILE;
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();
        discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        treeQueryBeamServiceHelper =  TreeQueryBeamServiceHelper.builder()
                .cacheTypeEnum(cacheTypeEnum)
                .avroSchemaHelper(avroSchemaHelper)
                .discoveryServiceInterface(discoveryServiceInterface)
                .treeQuerySetting(treeQuerySetting)
                .treeQueryClusterRunnerProxyInterface(LocalDummyTreeQueryClusterRunnerProxy.builder()
                        .treeQuerySetting(treeQuerySetting)
                        .cacheTypeEnum(cacheTypeEnum)
                        .avroSchemaHelper(avroSchemaHelper)
                        .build())
                .build();
        BindableService syncTreeQueryGrpcController = SyncTreeQueryGrpcController.builder()
                .treeQueryBeamServiceHelper(treeQueryBeamServiceHelper).build();
        BindableService[] bindableServices = {new SyncHealthCheckGrpcController(), syncTreeQueryGrpcController};



        WebServer webServer = new WebServer(treeQuerySetting.getServicePort(), Arrays.asList(bindableServices));
        webServer.start();
        webServer.blockUntilShutdown();
    }
}
