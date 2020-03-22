package org.treequery.cluster;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.treequery.model.Node;
import lombok.Builder;

import java.io.Serializable;
import java.util.*;

public class ClusterDependencyGraph implements Serializable {
    volatile Map<Node, Set> clusterDepGraph;
    Map<Node, Node> cacheDependency;

    ClusterDependencyGraph(Map<Node, Set> clusterDepGraph, Map<Node, Node> cacheDependency) {
        this.clusterDepGraph = clusterDepGraph;
        this.cacheDependency = cacheDependency;
    }

    public static ClusterDependencyGraphBuilder builder() {
        return new ClusterDependencyGraphBuilder();
    }

    public List<Node> findClusterWithoutDependency(){
        List<Node> result = Lists.newLinkedList();
        synchronized (this.clusterDepGraph){
            this.clusterDepGraph.entrySet().forEach(
                    entry->{
                        Set set = entry.getValue();
                        Node node = entry.getKey();
                        if (set.size() == 0){
                            result.add(node);
                        }
                    }
            );
        }
        result.sort((a,b)->a.getDescription().compareTo(b.getDescription()));
        return result;
    }
    public Node removeClusterDependency (Node node){
        Node parentClusterNode = null;
        synchronized (this.clusterDepGraph){
            parentClusterNode = this.cacheDependency.get(node);
            if (this.clusterDepGraph.get(node).size()==0){
                this.clusterDepGraph.remove(node);
            }
            if (parentClusterNode == null){
                return parentClusterNode;
            }
            Optional.of(this.clusterDepGraph.get(parentClusterNode)).orElseThrow().remove(node);
        }
        return parentClusterNode;
    }


    public static class ClusterDependencyGraphBuilder {
        private Map<Node, Set> clusterDepGraph = Maps.newHashMap();
        private Map<Node, Node> cacheDependency = Maps.newHashMap();

        ClusterDependencyGraphBuilder() {}

        public void constructDependencyGraph(Node rootNode){
            Stack<DepCluster> stack = new Stack<>();

            stack.push(DepCluster.builder()
                    .node(rootNode)
                    .parentClusterNode(null)
                    .build());
            while(stack.size() > 0){
                DepCluster depCluster = stack.pop();
                Node node = depCluster.node;
                Node parentClusterNode = depCluster.parentClusterNode;
                /*
                if (parentClusterNode == null){
                    this.clusterDepGraph.put(node, Sets.newHashSet());
                    this.cacheDependency.put(node, null);
                    this.__insertChildren2Stack(stack, node, node);
                }else */
                if(parentClusterNode!=null && parentClusterNode.isSameCluster(node)){
                    this.__insertChildren2Stack(stack, node, parentClusterNode);
                }else{
                    //Optional.of(this.clusterDepGraph.get(parentClusterNode)).get().add(node);
                    this.clusterDepGraph.computeIfPresent(parentClusterNode, (kNode, childSet)->{
                       childSet.add(node);
                       return childSet;
                    });
                    this.clusterDepGraph.put(node, Sets.newHashSet());
                    this.cacheDependency.put(node, parentClusterNode);
                    this.__insertChildren2Stack(stack, node, node);
                }
            }
        }

        private static void __insertChildren2Stack(Stack<DepCluster> stack, Node node, Node parentClusterNode){
            node.getChildren().forEach(
                    cNode->stack.push(DepCluster.builder().node(cNode).parentClusterNode(parentClusterNode).build())
            );
        }

        public ClusterDependencyGraph build() {
            return new ClusterDependencyGraph(clusterDepGraph, cacheDependency);
        }

        public String toString() {
            return "ClusterDependencyGraph.ClusterDependencyGraphBuilder(clusterDepGraph=" + this.clusterDepGraph + ", cacheDependency=" + this.cacheDependency + ")";
        }
    }

    @Builder
    private static class DepCluster{
        private final Node node;
        private final Node parentClusterNode;
    }

    public static ClusterDependencyGraph createClusterDependencyGraph(Node rootNode){
        ClusterDependencyGraph.ClusterDependencyGraphBuilder clusterDependencyGraphBuilder = ClusterDependencyGraph.builder();
        clusterDependencyGraphBuilder.constructDependencyGraph(rootNode);
        ClusterDependencyGraph clusterDependencyGraph = clusterDependencyGraphBuilder.build();
        return clusterDependencyGraph;
    }
}
