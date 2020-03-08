package io.exp.treequery.execute;

import io.exp.treequery.cluster.Cluster;
import io.exp.treequery.model.Node;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
@Slf4j
public class NodeTraverser {
    public static List<Node> postOrderTraversalExecution (Node node, Node parentNode, List<Node> jobList,NodePipeline nodePipeline){
        Cluster parentCluster = node.getCluster();
        node.getChildren().forEach(
                child->{
                    if (parentCluster.equals(child.getCluster())){
                        postOrderTraversalExecution(child, node, jobList, nodePipeline);
                    }else{
                        nodePipeline.addNodeToPipeline(child, node);
                    }
                }
        );
        nodePipeline.addNodeToPipeline(node, parentNode);
        jobList.add(node);
        return jobList;
    }
}
