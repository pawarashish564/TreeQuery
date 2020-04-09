package org.treequery.grpc.controller;

import io.grpc.stub.StreamObserver;
import org.treequery.proto.HealthCheckRequest;
import org.treequery.proto.HealthCheckResponse;
import org.treequery.proto.TreeQueryServiceGrpc;

public class TreeQuerySyncGrpcController extends TreeQueryServiceGrpc.TreeQueryServiceImplBase{
    @Override
    public void check(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
        HealthCheckResponse res = HealthCheckResponse.newBuilder().setStatus(HealthCheckResponse.ServingStatus.SERVING).build();
        responseObserver.onNext(res);
        responseObserver.onCompleted();
    }
}
