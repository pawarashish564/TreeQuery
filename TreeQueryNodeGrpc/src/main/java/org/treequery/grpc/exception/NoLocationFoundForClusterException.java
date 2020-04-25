package org.treequery.grpc.exception;

import org.treequery.cluster.Cluster;

public class NoLocationFoundForClusterException extends RuntimeException {
    public NoLocationFoundForClusterException(Cluster cluster){
        super (String.format("%s:No location found for cluster:%s",NoLocationFoundForClusterException.class.getName(),cluster.toString()));
    }
}
