package org.treequery.grpc.server;

import lombok.extern.slf4j.Slf4j;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.grpc.utils.WebServerFactory;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.TreeQuerySettingHelper;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public class Main {


    public static void main(String [] args) throws IOException, InterruptedException {
        TreeQuerySetting treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
        DiscoveryServiceInterface discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface =null;
        WebServer webServer = WebServerFactory.createWebServer(
                treeQuerySetting,
                discoveryServiceInterface,
                Optional.of(treeQueryClusterRunnerProxyInterface).get());
        webServer.start();
        webServer.blockUntilShutdown();
    }

}
