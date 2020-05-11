package org.treequery.grpc.server;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;
import org.treequery.grpc.utils.WebServerFactory;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.proxy.GrpcTreeQueryClusterRunnerProxy;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.TreeQuerySettingHelper;

import java.io.IOException;

@Slf4j
@SpringBootApplication
public class Main {
    static TreeQueryRequest.RunMode RUNMODE = TreeQueryRequest.RunMode.DIRECT;
    static boolean RENEW_CACHE = false;


    private static DiscoveryServiceInterface getDiscoveryServiceProxy(){
//        DiscoveryServiceInterface discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        DiscoveryServiceInterface discoveryServiceInterface = new DiscoveryServiceProxyImpl();

        return discoveryServiceInterface;
    }

    private static TreeQueryClusterRunnerProxyInterface createRemoteProxy(DiscoveryServiceInterface discoveryServiceInterface){
        return GrpcTreeQueryClusterRunnerProxy.builder()
                .discoveryServiceInterface(discoveryServiceInterface)
                .runMode(RUNMODE)
                .renewCache(RENEW_CACHE)
                .build();
    }
    private static WebServer startTreeQueryServer(String treeQueryYaml){
        TreeQuerySetting treeQuerySetting = TreeQuerySettingHelper.createFromYaml(treeQueryYaml, false);
        log.info("Run with following configuration:" + treeQuerySetting.toString());
        DiscoveryServiceInterface discoveryServiceInterface = getDiscoveryServiceProxy();
        TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface = createRemoteProxy(discoveryServiceInterface);


        WebServer webServer = WebServerFactory.createWebServer(
                treeQuerySetting,
                discoveryServiceInterface,
                treeQueryClusterRunnerProxyInterface
        );
        discoveryServiceInterface.registerCluster(treeQuerySetting.getCluster(),
                treeQuerySetting.getServicehostname(),
                treeQuerySetting.getServicePort());

        return webServer;
    }



    public static void main(String [] args) throws IOException, InterruptedException {
        String treeQueryYaml = args[0];
        WebServer webServer = startTreeQueryServer(treeQueryYaml);
        webServer.start();
        webServer.blockUntilShutdown();
    }


}
