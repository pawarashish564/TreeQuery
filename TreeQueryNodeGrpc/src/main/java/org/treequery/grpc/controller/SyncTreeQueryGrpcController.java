package org.treequery.grpc.controller;

import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.proto.TreeQueryResponse;
import org.treequery.proto.TreeQueryResponseHeader;
import org.treequery.proto.TreeQueryServiceGrpc;
import org.treequery.service.StatusTreeQueryCluster;
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

        TreeQueryBeamServiceHelper.ReturnResult returnResult = treeQueryBeamServiceHelper.process(
                RUNMODE,
                jsonRequest,
                renewCache,
                pageSize,
                page,(record)->{

                });
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
}
