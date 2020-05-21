package org.treequery.grpc.controller;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.Getter;
import org.apache.avro.Schema;
import org.treequery.grpc.exception.SchemaGetException;
import org.treequery.grpc.service.TreeQueryCacheService;
import org.treequery.grpc.utils.DataConsumerIntoByteArray;
import org.treequery.proto.*;
import org.treequery.service.CacheResult;

import java.util.Optional;

@Builder
public class TreeQueryCacheGrpcController extends TreeQueryCacheServiceGrpc.TreeQueryCacheServiceImplBase {

    private final TreeQueryCacheService treeQueryCacheService;


    @Override
    public void streamGet(CacheStreamRequest request, StreamObserver<CacheStreamResponse> responseObserver) {
        String identifier = request.getIdentifier();
        String avroSchemaString = request.getAvroSchema();
        Schema avroSchema = null;
        try{
            avroSchema = getSchema(avroSchemaString, identifier);
        }catch(SchemaGetException sge){
            responseObserver.onError(sge);
        }

        super.streamGet(request, responseObserver);
    }

    @Override
    public void get(TreeQueryCacheRequest request, StreamObserver<TreeQueryCacheResponse> responseObserver) {
        String identifier = request.getIdentifier();
        long pageSize = request.getPageSize();
        long page = request.getPage();
        String avroSchemaString = request.getAvroSchema();
        TreeQueryResponseResult.Builder treeQueryResponseResultBuilder = TreeQueryResponseResult.newBuilder();
        treeQueryResponseResultBuilder.setPageSize(pageSize);
        treeQueryResponseResultBuilder.setPage(page);

        Schema avroSchema = null;
        try{
            avroSchema = getPageSchema(avroSchemaString, identifier, pageSize, page);
        }catch(PageSchemaGetException sge){
            TreeQueryCacheResponse.Builder treeQueryCacheResponse = sge.getTreeQueryCacheResponseBuilder();
            responseObserver.onNext(treeQueryCacheResponse.build());
            responseObserver.onCompleted();
            return;
        }
        DataConsumerIntoByteArray dataConsumerIntoByteArray = new DataConsumerIntoByteArray(avroSchema);

        CacheResult cacheResult = treeQueryCacheService.getPage(identifier, pageSize, page, dataConsumerIntoByteArray);

        TreeQueryCacheResponse.Builder treeQueryCacheResponseBuilder =  TreeQueryCacheResponse.newBuilder();
        treeQueryCacheResponseBuilder.setRequestIdentifier(identifier);
        treeQueryCacheResponseBuilder = prepareHeaderResponse(cacheResult);


        TreeQueryResponseResult.Builder treeQueryResponseDataBuilder = TreeQueryResponseResult.newBuilder();
        ByteString avroLoad = ByteString.copyFrom(dataConsumerIntoByteArray.toArrayOutput());
        treeQueryResponseDataBuilder.setAvroLoad(avroLoad);
        treeQueryResponseDataBuilder.setDatasize(dataConsumerIntoByteArray.getDataSize());
        treeQueryResponseDataBuilder.setPage(page);
        treeQueryResponseDataBuilder.setPageSize(pageSize);
        treeQueryResponseDataBuilder.setAvroSchema(Optional.ofNullable(cacheResult.getDataSchema()).map(schema -> schema.toString()).orElse(""));
        treeQueryCacheResponseBuilder.setResult(treeQueryResponseDataBuilder.build());

        responseObserver.onNext(treeQueryCacheResponseBuilder.build());
        responseObserver.onCompleted();

    }

    Schema getSchema(String schemaString, String identifier) throws SchemaGetException {
        if (schemaString==null || schemaString.length()==0 ){
            try {
                return treeQueryCacheService.getSchemaOnly(identifier);
            }catch(Throwable t){
                throw new SchemaGetException(t.getMessage());
            }
        }
        Schema.Parser parser = new Schema.Parser();
        try {
            Schema schema = parser.parse(schemaString);
            return schema;
        }catch(Throwable t){
            throw new SchemaGetException(t.getMessage());
        }
    }
    Schema getPageSchema(String schemaString, String identifier, long pageSize, long page) throws PageSchemaGetException {
        try {
            return getSchema(schemaString, identifier);
        }catch(Throwable t){
            throw new PageSchemaGetException(identifier, t, pageSize, page);
        }
    }

    static TreeQueryCacheResponse.Builder prepareHeaderResponse(CacheResult cacheResult){
        TreeQueryCacheResponse.Builder treeQueryCacheResponse =  TreeQueryCacheResponse.newBuilder();
        treeQueryCacheResponse.setRequestIdentifier(cacheResult.getIdentifier());

        TreeQueryResponseHeader.Builder headerBuilder = TreeQueryResponseHeader.newBuilder();
        headerBuilder.setErrCode(cacheResult.getQueryTypeEnum().getValue());
        headerBuilder.setErrMsg(cacheResult.getDescription());
        headerBuilder.setSuccess(cacheResult.getQueryTypeEnum() == CacheResult.QueryTypeEnum.SUCCESS);
        treeQueryCacheResponse.setHeader(headerBuilder.build());


        return treeQueryCacheResponse;
    }

    static class PageSchemaGetException extends Exception{
        @Getter
        TreeQueryCacheResponse.Builder treeQueryCacheResponseBuilder= null;
        PageSchemaGetException(CacheResult cacheResult, long pageSize, long page){
            treeQueryCacheResponseBuilder = prepareHeaderResponse(cacheResult);
            treeQueryCacheResponseBuilder.setResult(setNullPageResponse(pageSize, page).build());
        }
        PageSchemaGetException(String identifier, Throwable throwable, long pageSize, long page){
            treeQueryCacheResponseBuilder =  TreeQueryCacheResponse.newBuilder();
            treeQueryCacheResponseBuilder.setRequestIdentifier(identifier);
            TreeQueryResponseHeader.Builder headerBuilder = TreeQueryResponseHeader.newBuilder();
            headerBuilder.setErrCode(CacheResult.QueryTypeEnum.FAIL.getValue());
            headerBuilder.setErrMsg(throwable.getMessage());
            headerBuilder.setSuccess(false);
            treeQueryCacheResponseBuilder.setHeader(headerBuilder.build());
            treeQueryCacheResponseBuilder.setResult(setNullPageResponse(pageSize, page).build());
        }

    }

    static TreeQueryResponseResult.Builder setNullPageResponse(long pageSize, long page){
        TreeQueryResponseResult.Builder treeQueryResponseResultBuilder = TreeQueryResponseResult.newBuilder();
        treeQueryResponseResultBuilder.setPageSize(pageSize);
        treeQueryResponseResultBuilder.setPage(page);
        treeQueryResponseResultBuilder.setDatasize(0);
        treeQueryResponseResultBuilder.setAvroSchema("");
        treeQueryResponseResultBuilder.setAvroLoad(ByteString.copyFrom(new byte[0]));
        return treeQueryResponseResultBuilder;
    }
}
