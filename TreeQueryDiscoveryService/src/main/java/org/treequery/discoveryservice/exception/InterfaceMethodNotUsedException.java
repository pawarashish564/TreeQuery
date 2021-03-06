package org.treequery.discoveryservice.exception;

public class InterfaceMethodNotUsedException extends RuntimeException {
    public InterfaceMethodNotUsedException(String method) {
        super(String.format("%s: %s Method is not needed for the implementation class.", InterfaceMethodNotUsedException.class.getName(), method));
    }
}