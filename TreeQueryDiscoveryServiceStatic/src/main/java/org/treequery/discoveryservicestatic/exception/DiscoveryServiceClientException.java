package org.treequery.discoveryservicestatic.exception;

public class DiscoveryServiceClientException extends RuntimeException {
    public DiscoveryServiceClientException (String msg) {
        super("Discovery Service Client throws exceptions: " + msg);
    }
}
