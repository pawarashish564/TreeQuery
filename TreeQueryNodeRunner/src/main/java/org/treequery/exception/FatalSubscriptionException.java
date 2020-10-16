package org.treequery.exception;


import lombok.Getter;

public class FatalSubscriptionException extends RuntimeException{
    @Getter
    protected final String listenerId;
    public FatalSubscriptionException(String listenerId, String msg){
        super(msg);
        this.listenerId = listenerId;
    }
}
