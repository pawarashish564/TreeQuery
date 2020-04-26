package org.treequery.grpc.exception;


import org.treequery.cluster.Cluster;

public class NodeNotMatchingGrpcServiceClusterException extends RuntimeException {
    public NodeNotMatchingGrpcServiceClusterException(Cluster nodeCluster, Cluster serviceClsuter){
        super(String.format("Node cluster %s not matching Service cluster %s", nodeCluster, serviceClsuter));
    }
}
