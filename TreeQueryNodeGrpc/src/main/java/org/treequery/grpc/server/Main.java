package org.treequery.grpc.server;

import io.grpc.BindableService;
import lombok.extern.slf4j.Slf4j;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.grpc.controller.SyncHealthCheckGrpcController;
import org.treequery.grpc.controller.SyncTreeQueryGrpcController;
import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.grpc.utils.WebServerFactory;
import org.treequery.service.proxy.LocalDummyTreeQueryClusterRunnerProxy;
import org.treequery.utils.BasicAvroSchemaHelperImpl;
import org.treequery.model.CacheTypeEnum;
import org.treequery.utils.AvroSchemaHelper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

@Slf4j
public class Main {


    public static void main(String [] args) throws IOException, InterruptedException {
        WebServer webServer = WebServerFactory.createLocalDummyWebServer(createTreeQuerySetting());
        webServer.start();
        webServer.blockUntilShutdown();
    }


    public static TreeQuerySetting createTreeQuerySetting(){
        TreeQuerySetting.TreeQuerySettingBuilder treeQuerySettingBuilder = TreeQuerySetting.builder();
        treeQuerySettingBuilder.servicehostname("localhost");
        treeQuerySettingBuilder.servicePort(9002);
        treeQuerySettingBuilder.cluster(Cluster.builder().clusterName("A").build());
        try {
            Path path = Files.createTempDirectory("TreeQuery_");
            log.info(String.format("Write cache File to path: %s", path.toAbsolutePath().toString()));
            treeQuerySettingBuilder.cacheFilePath(path.toAbsolutePath().toString());
        }catch(Exception ex){}
        return treeQuerySettingBuilder.build();
    }
}
