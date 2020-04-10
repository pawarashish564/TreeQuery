package org.treequery.grpc.server;

import io.grpc.BindableService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.treequery.grpc.client.HealthWebClient;
import org.treequery.grpc.controller.SyncHealthCheckGrpcController;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;
@Slf4j
class HealthWebServerTest {
    static WebServer webServer;
    final static int PORT = 9001;//ThreadLocalRandom.current().nextInt(9000,9999);
    final static String HOSTNAME = "localhost";
    @BeforeAll
    static void init() throws Exception{
        BindableService[] bindableServices = {new SyncHealthCheckGrpcController()};

        webServer = new WebServer(PORT, Arrays.asList(bindableServices));
        webServer.start();
        //webServer.blockUntilShutdown();
    }
    @Test
    void checkClient() {
        HealthWebClient healthWebClient = new HealthWebClient(HOSTNAME, PORT);
        boolean checkStatus = healthWebClient.healthCheck();
        assertTrue(checkStatus);
        log.info(String.format("Web client health check %b", checkStatus));
    }
    @AfterAll
    static void finish() throws Exception{
        log.info("All testing finish");
        webServer.stop();
    }
}