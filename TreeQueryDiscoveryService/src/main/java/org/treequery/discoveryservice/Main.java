package org.treequery.discoveryservice;

import org.springframework.boot.SpringApplication;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;

import java.io.IOException;
import java.util.Optional;

public class Main {
    public static void main(String [] args) throws IOException, InterruptedException {
//        SpringApplication.run(Main.class, args);

//        DiscoveryServiceInterface discoveryServiceInterface = new LocalDummyDiscoveryServiceProxy();
        DiscoveryServiceInterface discoveryServiceInterface = new DiscoveryServiceProxyImpl();
    }
}
