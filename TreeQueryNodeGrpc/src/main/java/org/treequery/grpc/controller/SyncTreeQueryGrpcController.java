package org.treequery.grpc.controller;

import com.google.common.collect.Lists;
import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.proto.TreeQueryResponse;
import org.treequery.proto.TreeQueryResponseHeader;
import org.treequery.proto.TreeQueryServiceGrpc;
import org.treequery.service.StatusTreeQueryCluster;

import java.util.List;
import java.util.function.Consumer;

//Reference: https://www.programcreek.com/java-api-examples/?api=org.apache.avro.io.BinaryEncoder
@Slf4j
@Builder
public class SyncTreeQueryGrpcController extends TreeQueryServiceGrpc.TreeQueryServiceImplBase {
    private static TreeQueryRequest.RunMode RUNMODE= TreeQueryRequest.RunMode.DIRECT;
    private final TreeQueryBeamServiceHelper treeQueryBeamServiceHelper;

    @Override
    public void query(TreeQueryRequest request, StreamObserver<TreeQueryResponse> responseObserver) {
        TreeQueryResponse.Builder treeQueryResponseBuilder = TreeQueryResponse.newBuilder();

        TreeQueryRequest.RunMode runMode = request.getRunMode();
        String jsonRequest = request.getJsonInput();
        boolean renewCache = request.getRenewCache();
        long pageSize = request.getPageSize();
        long page = request.getPage();


        DataConsumerIntoByteArray dataConsumerIntoByteArray = new DataConsumerIntoByteArray();

        TreeQueryBeamServiceHelper.PreprocessInput preprocessInput = treeQueryBeamServiceHelper.preprocess(jsonRequest);

        TreeQueryBeamServiceHelper.ReturnResult returnResult = treeQueryBeamServiceHelper.process(
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



        responseObserver.onNext(treeQueryResponseBuilder.build());
        responseObserver.onCompleted();
    }


    private static class DataConsumerIntoByteArray implements Consumer<GenericRecord> {
        @Getter
        List<GenericRecord> genericRecordList = Lists.newLinkedList();



        @Override
        public void accept(GenericRecord genericRecord) {

            genericRecordList.add(genericRecord);
        }
    }
}
