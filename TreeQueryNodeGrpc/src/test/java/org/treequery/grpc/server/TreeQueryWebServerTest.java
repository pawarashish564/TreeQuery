package org.treequery.grpc.server;

import io.grpc.BindableService;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.treequery.grpc.client.HealthWebClient;
import org.treequery.grpc.client.TreeQueryClient;
import org.treequery.grpc.controller.SyncHealthCheckGrpcController;
import org.treequery.grpc.controller.SyncTreeQueryGrpcController;
import org.treequery.grpc.model.TreeQueryResult;
import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.grpc.utils.TestDataAgent;
import org.treequery.model.CacheTypeEnum;
import org.treequery.proto.TreeQueryRequest;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.GenericArrayType;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
@Slf4j
class TreeQueryWebServerTest {
    static WebServer webServer;
    final static int PORT = 9002;//ThreadLocalRandom.current().nextInt(9000,9999);
    final static String HOSTNAME = "localhost";
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
    void healthCheckClient() {
        HealthWebClient healthWebClient = new HealthWebClient(HOSTNAME, PORT);
        boolean checkStatus = healthWebClient.healthCheck();
        assertTrue(checkStatus);
        log.info(String.format("Web client health check %b", checkStatus));
    }

    @Test
    void happyPathSimpleJoin(){
        String AvroTree = "SimpleJoin.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        TreeQueryClient treeQueryClient = new TreeQueryClient(HOSTNAME, PORT);

        boolean renewCache = true;
        int pageSize = 3;
        int page = 2;
        TreeQueryResult treeQueryResult = treeQueryClient.query(TreeQueryRequest.RunMode.DIRECT,
                jsonString,
                true,
                pageSize,
                page
                );
        assertTrue(treeQueryResult.getHeader().isSuccess());
        assertEquals(0,treeQueryResult.getHeader().getErr_code());
        TreeQueryResult.TreeQueryResponseResult treeQueryResponseResult = treeQueryResult.getResult();
        assertEquals(3, treeQueryResponseResult.getDatasize());
        List<GenericRecord> genericRecordList = treeQueryResult.getResult().getGenericRecordList();
        genericRecordList.forEach(
                genericRecord -> {
                    assertThat(genericRecord).isNotNull();
                    assertThat(genericRecord.get("bondtrade")).isNotNull();
                }
        );

    }


    @Test
    void testByteStream() throws Exception{
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte [] byteData = byteArrayOutputStream.toByteArray();
        assertNotNull(byteData);
        byteArrayOutputStream.close();
    }

    @AfterAll
    static void finish() throws Exception{
        log.info("All testing finish");
        webServer.stop();
    }
}