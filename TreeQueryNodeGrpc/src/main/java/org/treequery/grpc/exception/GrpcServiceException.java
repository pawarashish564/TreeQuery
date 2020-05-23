package org.treequery.grpc.exception;

import io.grpc.Status;

public class GrpcServiceException extends Exception {
    public GrpcServiceException(Status.Code statusCode, String message){
        super(String.format("%s:%s", statusCode.toString(), message));
    }
}
