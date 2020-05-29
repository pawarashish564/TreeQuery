package org.treequery.exception;

public class CacheNotFoundException extends RuntimeException {
    public CacheNotFoundException(String msg){
        super(msg);
    }
}
