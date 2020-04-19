package org.treequery.grpc.controller;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.treequery.grpc.service.TreeQueryCacheService;
import org.treequery.grpc.service.TreeQueryCacheServiceHelper;
import org.treequery.proto.*;
import org.treequery.service.CacheResult;

import java.util.Optional;

@Builder
public class SyncTreeQueryCacheGrpcController extends TreeQueryCacheServiceGrpc.TreeQueryCacheServiceImplBase {

    private final TreeQueryCacheService treeQueryCacheService;

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
            avroSchema = getSchemaFromString(avroSchemaString, identifier);
        }catch(SchemaGetException sge){
            TreeQueryCacheResponse.Builder treeQueryCacheResponse = sge.getTreeQueryCacheResponseBuilder();
            responseObserver.onNext(treeQueryCacheResponse.build());
            responseObserver.onCompleted();
            return;
        }



    }

    Schema getSchemaFromString (String schemaString, String identifier) throws SchemaGetException{
        if (schemaString==null || schemaString.length()==0 ){
            CacheResult cacheResult = treeQueryCacheService.get(identifier, 1, 1, (record)->{});
            if (cacheResult.getQueryTypeEnum()== CacheResult.QueryTypeEnum.SUCCESS) {
                return cacheResult.getDataSchema();
            }else{
                throw new SchemaGetException(cacheResult);
            }
        }
        Schema.Parser parser = new Schema.Parser();
        try {
            Schema schema = parser.parse(schemaString);
            return schema;
        }catch(Throwable t){
            throw new SchemaGetException(identifier, t);
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

    static class SchemaGetException extends Exception{
        @Getter
        TreeQueryCacheResponse.Builder treeQueryCacheResponseBuilder= null;
        SchemaGetException( CacheResult cacheResult){
            treeQueryCacheResponseBuilder = prepareHeaderResponse(cacheResult);
            treeQueryCacheResponseBuilder.setResult(setNullResponse().build());
        }
        SchemaGetException( String identifier, Throwable throwable){
            treeQueryCacheResponseBuilder =  TreeQueryCacheResponse.newBuilder();
            treeQueryCacheResponseBuilder.setRequestIdentifier(identifier);
            TreeQueryResponseHeader.Builder headerBuilder = TreeQueryResponseHeader.newBuilder();
            headerBuilder.setErrCode(CacheResult.QueryTypeEnum.FAIL.getValue());
            headerBuilder.setErrMsg(throwable.getMessage());
            headerBuilder.setSuccess(false);
            treeQueryCacheResponseBuilder.setHeader(headerBuilder.build());
            treeQueryCacheResponseBuilder.setResult(setNullResponse().build());
        }

    }

    static TreeQueryResponseResult.Builder setNullResponse(){
        TreeQueryResponseResult.Builder treeQueryResponseResultBuilder = TreeQueryResponseResult.newBuilder();
        treeQueryResponseResultBuilder.setPageSize(0);
        treeQueryResponseResultBuilder.setPage(0);
        treeQueryResponseResultBuilder.setDatasize(0);
        treeQueryResponseResultBuilder.setAvroSchema("");
        treeQueryResponseResultBuilder.setAvroLoad(ByteString.copyFrom(new byte[0]));
        return treeQueryResponseResultBuilder;
    }
}
