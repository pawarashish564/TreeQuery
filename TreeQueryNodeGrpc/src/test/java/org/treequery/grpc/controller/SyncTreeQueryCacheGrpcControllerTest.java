package org.treequery.grpc.controller;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.config.TreeQuerySetting;
import org.treequery.grpc.service.TreeQueryCacheService;
import org.treequery.grpc.service.TreeQueryCacheServiceHelper;
import org.treequery.grpc.utils.GenericRecordReader;
import org.treequery.grpc.utils.TestDataAgent;
import org.treequery.model.CacheTypeEnum;
import org.treequery.proto.TreeQueryCacheRequest;
import org.treequery.proto.TreeQueryCacheResponse;
import org.treequery.proto.TreeQueryResponse;
import org.treequery.service.CacheResult;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
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
            syncTreeQueryCacheGrpcController.getSchemaFromString("xys", treeQueryCacheRequest.getIdentifier(), treeQueryCacheRequest.getPageSize(), treeQueryCacheRequest.getPage());
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
            syncTreeQueryCacheGrpcController.getSchemaFromString(treeQueryCacheRequest.getAvroSchema(), "xyz", treeQueryCacheRequest.getPageSize(), treeQueryCacheRequest.getPage());
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
            Schema schema = syncTreeQueryCacheGrpcController.getSchemaFromString(treeQueryCacheRequest.getAvroSchema(), treeQueryCacheRequest.getIdentifier(), treeQueryCacheRequest.getPageSize(), treeQueryCacheRequest.getPage());
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

    @Test
    void HappyPathtoGetRecord() {
        long page = 1;
        long pageSize = 100;
        AtomicLong counter = new AtomicLong(0);
        do {
            long inx = counter.get();
            treeQueryCacheRequest = TreeQueryCacheRequest.newBuilder()
                    .setAvroSchema("")
                    .setIdentifier(identifier)
                    .setPage(page)
                    .setPageSize(pageSize)
                    .build();

            StreamObserver<TreeQueryCacheResponse> responseObserver = new StreamObserver<TreeQueryCacheResponse>() {
                @Override
                public void onNext(TreeQueryCacheResponse treeQueryCacheResponse) {
                    assertEquals(CacheResult.QueryTypeEnum.SUCCESS.getValue(), treeQueryCacheResponse.getHeader().getErrCode());
                    long dataSize = treeQueryCacheResponse.getResult().getDatasize();
                    ByteString dataLoadString = treeQueryCacheResponse.getResult().getAvroLoad();
                    String avroSchemaStr = treeQueryCacheResponse.getResult().getAvroSchema();
                    Schema schema = new Schema.Parser().parse(avroSchemaStr);
                    try {
                        GenericRecordReader.readGenericRecordFromProtoByteString(dataLoadString, schema,
                                (genericRecord -> {
                                    counter.incrementAndGet();
                                    assertThat(genericRecord).isNotNull();
                                    assertThat(genericRecord.get("bondtrade")).isNotNull();
                                }));
                    }catch(IOException ioe){
                        throw new IllegalStateException(ioe.getMessage());
                    }
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onCompleted() {
                }
            };
            syncTreeQueryCacheGrpcController.get(treeQueryCacheRequest, responseObserver);
            if (counter.get() == inx){
                break;
            }
            page++;
        }while(true);
        assertEquals(1000, counter.get());

    }
}