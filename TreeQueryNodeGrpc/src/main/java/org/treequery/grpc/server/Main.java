package org.treequery.grpc.server;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;
import org.treequery.discoveryservice.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.grpc.utils.WebServerFactory;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.TreeQuerySettingHelper;

import java.io.IOException;
import java.util.Optional;

@Slf4j
@SpringBootApplication(scanBasePackages = {"org.treequery.grpc.controller"})
@EnableDiscoveryClient
public class Main {
    public static final TreeQuerySetting treeQuerySetting = TreeQuerySettingHelper.createFromYaml();

    public static void main(String [] args) throws IOException, InterruptedException {
        SpringApplication.run(Main.class, args);

//        DiscoveryServiceInterface discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        DiscoveryServiceInterface discoveryServiceInterface = new DiscoveryServiceProxyImpl();
        TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface =null;

        WebServer webServer = WebServerFactory.createWebServer(
                treeQuerySetting,
                discoveryServiceInterface,
                Optional.of(treeQueryClusterRunnerProxyInterface).get());
        webServer.start();
        webServer.blockUntilShutdown();
    }
}
