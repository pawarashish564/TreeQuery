package io.exp.treequery.service;

import io.exp.treequery.cluster.ClusterDependencyGraph;
import io.exp.treequery.model.Node;
import lombok.Builder;

import java.util.List;
import java.util.function.Consumer;


@Builder
public class SimpleLocalTreeQueryClusterRunnerImpl implements TreeQueryClusterRunner {
    @Override
    public void runQueryTreeNetwork(Node rootNode, Consumer<StatusTreeQueryCluster> statusCallback) {
        ClusterDependencyGraph clusterDependencyGraph = ClusterDependencyGraph.createClusterDependencyGraph(rootNode);

        while (true){
            List<Node> nodeList = clusterDependencyGraph.findClusterWithoutDependency();
            if (nodeList.size()==0){
                break;
            }
            for (Node node: nodeList) {
                clusterDependencyGraph.removeClusterDependency(node);
            }
        }

        statusCallback.accept(new StatusTreeQueryCluster());
    }
}
