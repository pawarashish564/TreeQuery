package org.treequery.grpc.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.config.TreeQuerySetting;
import org.treequery.grpc.service.TreeQueryCacheService;
import org.treequery.grpc.service.TreeQueryCacheServiceHelper;
import org.treequery.grpc.utils.TestDataAgent;
import org.treequery.model.CacheTypeEnum;
import org.treequery.proto.TreeQueryCacheRequest;
import org.treequery.proto.TreeQueryCacheResponse;
import org.treequery.proto.TreeQueryResponse;

import static org.junit.jupiter.api.Assertions.*;
@Slf4j
class SyncTreeQueryCacheGrpcControllerTest {
    TreeQueryCacheRequest treeQueryCacheRequest = null;
    String identifier = "BondTradeJoinBondStatic";
    String avroSampleFile = identifier+".avro";
    TreeQuerySetting treeQuerySetting;
    TreeQueryCacheService treeQueryCacheService;
    SyncTreeQueryCacheGrpcController syncTreeQueryCacheGrpcController = null;
    String HOSTNAME = "localhost";
    int PORT = 9001;

    @BeforeEach
    void init(){
        CacheTypeEnum cacheTypeEnum = CacheTypeEnum.FILE;
        TreeQuerySetting.TreeQuerySettingBuilder treeQuerySettingBuilder = new TreeQuerySetting.TreeQuerySettingBuilder(
                "A",
                HOSTNAME,
                PORT,
                TestDataAgent.getTestResourceDirectory(avroSampleFile),
                "",0
        );
        treeQuerySetting = treeQuerySettingBuilder.build();

        treeQueryCacheService = TreeQueryCacheServiceHelper.builder()
                .cacheTypeEnum(cacheTypeEnum)
                .treeQuerySetting(treeQuerySetting)
                .build();

        syncTreeQueryCacheGrpcController = SyncTreeQueryCacheGrpcController.builder()
                                            .treeQueryCacheService(treeQueryCacheService)
                                            .build();
        treeQueryCacheRequest = TreeQueryCacheRequest.newBuilder()
                .setAvroSchema("")
                .setIdentifier(identifier)
                .setPage(1)
                .setPageSize(1)
                .build();
    }
    @Test
    void failToGetSchemaFromIncorrectSchemaString() {
        boolean throwException = false;
        try {
            syncTreeQueryCacheGrpcController.getSchemaFromString("xys", treeQueryCacheRequest.getIdentifier());
        }catch(SyncTreeQueryCacheGrpcController.SchemaGetException sge){
            TreeQueryCacheResponse.Builder treeQueryCacheResponseBuilder = sge.getTreeQueryCacheResponseBuilder();
            TreeQueryCacheResponse treeQueryCacheResponse = treeQueryCacheResponseBuilder.build();
            assertNotNull(treeQueryCacheResponse);
            throwException = true;
        }
        assertTrue(throwException);
    }

    @Test
    void failToGetSchemaFromWrongIdentifier() {
        boolean throwException = false;
        try {
            syncTreeQueryCacheGrpcController.getSchemaFromString(treeQueryCacheRequest.getAvroSchema(), "xyz");
        }catch(SyncTreeQueryCacheGrpcController.SchemaGetException sge){
            TreeQueryCacheResponse.Builder treeQueryCacheResponseBuilder = sge.getTreeQueryCacheResponseBuilder();
            TreeQueryCacheResponse treeQueryCacheResponse = treeQueryCacheResponseBuilder.build();
            assertNotNull(treeQueryCacheResponse);
            throwException = true;
        }
        assertTrue(throwException);
    }

    @Test
    void HappyPathToGetSchema() {
        boolean throwException = false;
        try {
            Schema schema = syncTreeQueryCacheGrpcController.getSchemaFromString(treeQueryCacheRequest.getAvroSchema(), treeQueryCacheRequest.getIdentifier());
            assertNotNull(schema);

            String schemaStr = schema.toString();
            Schema.Parser parser= new Schema.Parser();
            Schema schema1 = parser.parse(schemaStr);
            assertEquals(schema, schema1);
        }catch(SyncTreeQueryCacheGrpcController.SchemaGetException sge){
            TreeQueryCacheResponse.Builder treeQueryCacheResponseBuilder = sge.getTreeQueryCacheResponseBuilder();
            TreeQueryCacheResponse treeQueryCacheResponse = treeQueryCacheResponseBuilder.build();
            assertNotNull(treeQueryCacheResponse);
            throwException = true;
        }
        assertFalse(throwException);
    }
}