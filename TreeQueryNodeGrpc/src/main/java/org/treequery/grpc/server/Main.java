package org.treequery.grpc.server;

import io.grpc.BindableService;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.grpc.controller.SyncHealthCheckGrpcController;
import org.treequery.grpc.controller.SyncTreeQueryGrpcController;
import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.model.BasicAvroSchemaHelperImpl;
import org.treequery.model.CacheTypeEnum;
import org.treequery.utils.AvroSchemaHelper;

import java.io.IOException;
import java.util.Arrays;


public class Main {
    static TreeQueryBeamServiceHelper treeQueryBeamServiceHelper;
    static DiscoveryServiceInterface discoveryServiceInterface;
    static AvroSchemaHelper avroSchemaHelper;

    public static void main(String [] args) throws IOException, InterruptedException {

        avroSchemaHelper = new BasicAvroSchemaHelperImpl();
        discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        treeQueryBeamServiceHelper =  TreeQueryBeamServiceHelper.builder()
                .cacheTypeEnum(CacheTypeEnum.FILE)
                .avroSchemaHelper(avroSchemaHelper)
                .discoveryServiceInterface(discoveryServiceInterface)
                .build();
        BindableService syncTreeQueryGrpcController = SyncTreeQueryGrpcController.builder()
                .treeQueryBeamServiceHelper(treeQueryBeamServiceHelper).build();
        BindableService[] bindableServices = {new SyncHealthCheckGrpcController(), syncTreeQueryGrpcController};


        int PORT = 9001;
        WebServer webServer = new WebServer(PORT, Arrays.asList(bindableServices));
        webServer.start();
        webServer.blockUntilShutdown();
    }
}
