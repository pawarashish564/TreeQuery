package org.treequery.grpc.exception;

import org.treequery.cluster.Cluster;

public class NoLocationFoundForClusterException extends RuntimeException {
    public NoLocationFoundForClusterException(Cluster cluster){
        super (String.format("No location found for cluster:%s",cluster.toString()));
    }
}
