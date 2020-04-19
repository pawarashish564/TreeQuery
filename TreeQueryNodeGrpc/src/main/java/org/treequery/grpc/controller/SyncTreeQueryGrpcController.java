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
public class SyncTreeQueryGrpcController extends TreeQueryServiceGrpc.TreeQueryServiceImplBase {
    private static TreeQueryRequest.RunMode RUNMODE= TreeQueryRequest.RunMode.DIRECT;
    private final TreeQueryBeamService treeQueryBeamService;

    @Override
    public void query(TreeQueryRequest request, StreamObserver<TreeQueryResponse> responseObserver) {
        TreeQueryResponse.Builder treeQueryResponseBuilder = TreeQueryResponse.newBuilder();

        TreeQueryRequest.RunMode runMode = request.getRunMode();
        String jsonRequest = request.getJsonInput();
        boolean renewCache = request.getRenewCache();
        long pageSize = request.getPageSize();
        long page = request.getPage();

        PreprocessInput preprocessInput = treeQueryBeamService.preprocess(jsonRequest);
        Schema outputSchema = preprocessInput.getOutputSchema();
        DataConsumerIntoByteArray dataConsumerIntoByteArray = new DataConsumerIntoByteArray(outputSchema);

        ReturnResult returnResult = treeQueryBeamService.runAndPageResult(
                RUNMODE,
                preprocessInput,
                renewCache,
                pageSize,
                page,dataConsumerIntoByteArray);

        treeQueryResponseBuilder.setRequestHash(returnResult.getHashCode());

        TreeQueryResponseHeader.Builder headerBuilder = TreeQueryResponseHeader.newBuilder();
        StatusTreeQueryCluster statusTreeQueryCluster = returnResult.getStatusTreeQueryCluster();
        headerBuilder.setSuccess(statusTreeQueryCluster.getStatus()==StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
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
    }
}
