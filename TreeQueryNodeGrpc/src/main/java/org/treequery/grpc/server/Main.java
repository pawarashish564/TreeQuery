package org.treequery.grpc.server;

import lombok.extern.slf4j.Slf4j;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.grpc.utils.WebServerFactory;
import org.treequery.utils.TreeQuerySettingHelper;

import java.io.IOException;

@Slf4j
public class Main {


    public static void main(String [] args) throws IOException, InterruptedException {
        TreeQuerySetting treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
        DiscoveryServiceInterface discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        WebServer webServer = WebServerFactory.createWebServer(
                treeQuerySetting,
                discoveryServiceInterface);
        webServer.start();
        webServer.blockUntilShutdown();
    }

}
