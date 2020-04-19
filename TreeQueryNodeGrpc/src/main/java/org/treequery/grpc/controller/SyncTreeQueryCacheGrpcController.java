package org.treequery.grpc.controller;

import io.grpc.stub.StreamObserver;
import org.treequery.grpc.service.TreeQueryCacheServiceHelper;
import org.treequery.proto.TreeQueryCacheRequest;
import org.treequery.proto.TreeQueryCacheResponse;
import org.treequery.proto.TreeQueryCacheServiceGrpc;

public class SyncTreeQueryCacheGrpcController extends TreeQueryCacheServiceGrpc.TreeQueryCacheServiceImplBase {

    TreeQueryCacheServiceHelper treeQueryCacheServiceHelper;

    @Override
    public void get(TreeQueryCacheRequest request, StreamObserver<TreeQueryCacheResponse> responseObserver) {
        super.get(request, responseObserver);
    }
}
