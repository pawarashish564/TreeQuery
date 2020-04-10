package org.treequery.grpc.controller;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.proto.TreeQueryResponse;
import org.treequery.proto.TreeQueryServiceGrpc;

@Slf4j
public class SyncTreeQueryGrpcController extends TreeQueryServiceGrpc.TreeQueryServiceImplBase {
    @Override
    public void query(TreeQueryRequest request, StreamObserver<TreeQueryResponse> responseObserver) {
        TreeQueryResponse.Builder treeQueryResponseBuilder = TreeQueryResponse.newBuilder();

        String jsonRequest = request.getJsonInput();


        responseObserver.onNext(treeQueryResponseBuilder.build());
        responseObserver.onCompleted();
    }
}
