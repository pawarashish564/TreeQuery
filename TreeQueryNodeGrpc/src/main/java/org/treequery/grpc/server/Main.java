package org.treequery.grpc.server;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.Exception.InterfaceMethodNotUsedException;
import org.treequery.discoveryservice.client.DynamoClient;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;
import org.treequery.discoveryservice.proxy.LocalDummyDiscoveryServiceProxy;
import org.treequery.grpc.utils.WebServerFactory;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.proxy.GrpcTreeQueryClusterRunnerProxy;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.TreeQuerySettingHelper;

import java.io.IOException;

@Slf4j
@SpringBootApplication
@EnableEurekaClient
public class Main {
    static TreeQueryRequest.RunMode RUNMODE = TreeQueryRequest.RunMode.DIRECT;
    static boolean RENEW_CACHE = false;


    private static DiscoveryServiceInterface getDiscoveryServiceProxy() {
//        DiscoveryServiceInterface discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        DynamoDB dynamoDB = new DynamoClient("https://dynamodb.us-west-2.amazonaws.com").getDynamoDB();
        DiscoveryServiceInterface discoveryServiceInterface = new DiscoveryServiceProxyImpl(dynamoDB);
        return discoveryServiceInterface;
    }

    private static TreeQueryClusterRunnerProxyInterface createRemoteProxy(DiscoveryServiceInterface discoveryServiceInterface) {
        return GrpcTreeQueryClusterRunnerProxy.builder()
                .discoveryServiceInterface(discoveryServiceInterface)
                .runMode(RUNMODE)
                .renewCache(RENEW_CACHE)
                .build();
    }

    private static WebServer startTreeQueryServer(String treeQueryYaml) {
        TreeQuerySetting treeQuerySetting = TreeQuerySettingHelper.createFromYaml(treeQueryYaml, false);
        log.info("Run with following configuration:" + treeQuerySetting.toString());
        DiscoveryServiceInterface discoveryServiceInterface = getDiscoveryServiceProxy();
        TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface = createRemoteProxy(discoveryServiceInterface);

        WebServer webServer = WebServerFactory.createWebServer(
                treeQuerySetting,
                discoveryServiceInterface,
                treeQueryClusterRunnerProxyInterface
        );

        try {
            discoveryServiceInterface.registerCluster(treeQuerySetting.getCluster(),
                    treeQuerySetting.getServicehostname(),
                    treeQuerySetting.getServicePort());
        } catch (InterfaceMethodNotUsedException ex) {
            System.err.println(ex.getMessage());
        }

        return webServer;
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        SpringApplication.run(Main.class, args);
        String treeQueryYaml = args[0];
        WebServer webServer = startTreeQueryServer(treeQueryYaml);
        webServer.start();
        webServer.blockUntilShutdown();
    }


}
