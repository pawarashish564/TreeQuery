package org.treequery.grpc.controller;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.treequery.proto.HealthCheckRequest;
import org.treequery.proto.HealthCheckResponse;
import org.treequery.proto.HealthCheckServiceGrpc;
import org.treequery.proto.TreeQueryServiceGrpc;

@Slf4j
@GRpcService
public class SyncHealthCheckGrpcController extends HealthCheckServiceGrpc.HealthCheckServiceImplBase{
    @Override
    public void check(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
        log.info(String.format("health check request %s",request.toString()));
        HealthCheckResponse res = HealthCheckResponse.newBuilder().setStatus(HealthCheckResponse.ServingStatus.SERVING).build();
        responseObserver.onNext(res);
        responseObserver.onCompleted();
    }
}
