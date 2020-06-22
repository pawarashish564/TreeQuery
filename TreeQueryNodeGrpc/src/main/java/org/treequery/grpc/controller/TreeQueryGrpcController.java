package org.treequery.grpc.controller;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.treequery.grpc.service.TreeQueryBeamService;
import org.treequery.grpc.utils.DataConsumerIntoByteArray;
import org.treequery.model.Node;
import org.treequery.proto.*;
import org.treequery.service.PreprocessInput;
import org.treequery.service.ReturnResult;
import org.treequery.service.StatusTreeQueryCluster;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;

//Reference: https://www.programcreek.com/java-api-examples/?api=org.apache.avro.io.BinaryEncoder
@Slf4j
@Builder
public class TreeQueryGrpcController extends TreeQueryServiceGrpc.TreeQueryServiceImplBase {
    private static TreeQueryRequest.RunMode RUNMODE= TreeQueryRequest.RunMode.DIRECT;
    private final TreeQueryBeamService treeQueryBeamService;

    @Override
    public void queryByPageStream(TreeQueryRequest request, StreamObserver<TreeQueryResponseStream> responseObserver) {
        TreeQueryResponse.Builder treeQueryResponseBuilder = TreeQueryResponse.newBuilder();

        TreeQueryRequest.RunMode runMode = request.getRunMode();
        String jsonRequest = request.getJsonInput();
        boolean renewCache = request.getRenewCache();
        long pageSize = request.getPageSize();
        long page = request.getPage();
        
    }

    @Override
    public void queryByPage(TreeQueryRequest request, StreamObserver<TreeQueryResponse> responseObserver) {
        TreeQueryResponse.Builder treeQueryResponseBuilder = TreeQueryResponse.newBuilder();

        TreeQueryRequest.RunMode runMode = request.getRunMode();
        String jsonRequest = request.getJsonInput();
        boolean renewCache = request.getRenewCache();
        long pageSize = request.getPageSize();
        long page = request.getPage();

        try {
            treeQueryResponseBuilder.setRequestHash(Node.getHash(jsonRequest));
            PreprocessInput preprocessInput = treeQueryBeamService.preprocess(jsonRequest);
            Schema outputSchema = preprocessInput.getOutputSchema();
            DataConsumerIntoByteArray dataConsumerIntoByteArray = new DataConsumerIntoByteArray(outputSchema);

            ReturnResult returnResult = treeQueryBeamService.runAndPageResult(
                    RUNMODE,
                    preprocessInput,
                    renewCache,
                    pageSize,
                    page, dataConsumerIntoByteArray);

            treeQueryResponseBuilder.setRequestHash(returnResult.getHashCode());

            TreeQueryResponseHeader.Builder headerBuilder = TreeQueryResponseHeader.newBuilder();
            StatusTreeQueryCluster statusTreeQueryCluster = returnResult.getStatusTreeQueryCluster();
            headerBuilder.setSuccess(statusTreeQueryCluster.getStatus() == StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
            headerBuilder.setErrCode(statusTreeQueryCluster.getStatus().getValue());
            headerBuilder.setErrMsg(statusTreeQueryCluster.getDescription());

            treeQueryResponseBuilder.setHeader(headerBuilder.build());

            TreeQueryResponseResult.Builder treeQueryResponseDataBuilder = TreeQueryResponseResult.newBuilder();
            ByteString avroLoad = ByteString.copyFrom(dataConsumerIntoByteArray.toArrayOutput());
            treeQueryResponseDataBuilder.setAvroLoad(avroLoad);
            treeQueryResponseDataBuilder.setDatasize(dataConsumerIntoByteArray.getDataSize());
            treeQueryResponseDataBuilder.setPage(page);
            treeQueryResponseDataBuilder.setPageSize(pageSize);
            treeQueryResponseDataBuilder.setAvroSchema(Optional.ofNullable(returnResult.getDataSchema()).map(schema -> schema.toString()).orElse(""));
            treeQueryResponseBuilder.setResult(treeQueryResponseDataBuilder.build());


            responseObserver.onNext(treeQueryResponseBuilder.build());
            responseObserver.onCompleted();
        }catch(Throwable throwable){
            prepareSystemErrorException(treeQueryResponseBuilder, throwable);
            responseObserver.onNext(treeQueryResponseBuilder.build());
            responseObserver.onCompleted();
        }

    }


    static void prepareSystemErrorException(TreeQueryResponse.Builder treeQueryResponseBuilder, Throwable throwable){
        TreeQueryResponseHeader.Builder headerBuilder = TreeQueryResponseHeader.newBuilder();
        headerBuilder.setSuccess(false);
        headerBuilder.setErrCode(StatusTreeQueryCluster.QueryTypeEnum.SYSTEMERROR.getValue());
        headerBuilder.setErrMsg(throwable.getMessage());
        treeQueryResponseBuilder.setHeader(headerBuilder.build());

        TreeQueryResponseResult.Builder treeQueryResponseDataBuilder = TreeQueryResponseResult.newBuilder();
        treeQueryResponseDataBuilder.setAvroLoad(ByteString.EMPTY);
        treeQueryResponseDataBuilder.setDatasize(0);
        treeQueryResponseDataBuilder.setPage(0);
        treeQueryResponseDataBuilder.setPageSize(0);
        treeQueryResponseDataBuilder.setAvroSchema("");
        treeQueryResponseBuilder.setResult(treeQueryResponseDataBuilder.build());
    }

}
