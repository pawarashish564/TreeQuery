package org.treequery.grpc.server;

import io.grpc.BindableService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.treequery.grpc.controller.SyncHealthCheckGrpcController;
import org.treequery.grpc.controller.SyncTreeQueryGrpcController;
import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.grpc.utils.TestDataAgent;
import org.treequery.model.CacheTypeEnum;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
@Slf4j
class TreeQueryWebServerTest {
    static WebServer webServer;
    static int PORT = 9002;//ThreadLocalRandom.current().nextInt(9000,9999);

    static String jsonString;
    static TreeQueryBeamServiceHelper treeQueryBeamServiceHelper;
    @BeforeAll
    static void init() throws Exception{
        String AvroTree = "SimpleJoin.json";
        jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        treeQueryBeamServiceHelper = new TreeQueryBeamServiceHelper(CacheTypeEnum.FILE);

        BindableService syncTreeQueryGrpcController = SyncTreeQueryGrpcController.builder()
                .treeQueryBeamServiceHelper(treeQueryBeamServiceHelper).build();

        BindableService[] bindableServices = {new SyncHealthCheckGrpcController(), syncTreeQueryGrpcController};

        webServer = new WebServer(PORT, Arrays.asList(bindableServices));
        webServer.start();
        //webServer.blockUntilShutdown();
    }

    @Test
    void start() {
    }

    @AfterAll
    static void finish() throws Exception{
        log.info("All testing finish");
        webServer.stop();
    }
}