package org.treequery.grpc.exception;

public class FailConnectionException extends RuntimeException {
    public FailConnectionException(String msg){
        super(msg);
    }
}
