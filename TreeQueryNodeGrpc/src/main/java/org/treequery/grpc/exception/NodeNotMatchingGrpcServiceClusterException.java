package org.treequery.grpc.exception;


import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;

public class NodeNotMatchingGrpcServiceClusterException extends RuntimeException {
    public NodeNotMatchingGrpcServiceClusterException(Cluster nodeCluster, TreeQuerySetting serviceSetting){
        super(String.format("Node cluster %s not matching Service cluster %s serving at %s:%s", nodeCluster, serviceSetting.getCluster(), serviceSetting.getServicehostname(), serviceSetting.getServicePort()));
    }
}
