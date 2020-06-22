package org.treequery.discoveryservicestatic.exception;

public class EndPointException extends RuntimeException {
    public EndPointException (String msg) {
        super("Endpoint connection throws exceptions: " + msg);
    }
}
