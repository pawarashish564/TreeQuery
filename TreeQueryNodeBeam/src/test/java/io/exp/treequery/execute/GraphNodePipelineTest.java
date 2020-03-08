package io.exp.treequery.execute;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import io.exp.treequery.Transform.TransformNodeFactory;
import io.exp.treequery.cluster.ClusterDependencyGraph;
import io.exp.treequery.cluster.NodeFactory;
import io.exp.treequery.cluster.NodeTreeFactory;
import io.exp.treequery.model.Node;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
@Slf4j
class GraphNodePipelineTest {

    String fileName = "TreeQueryInput3.json";
    NodeFactory nodeFactory;
    NodeTreeFactory nodeTreeFactory;
    Node node = null;

    PipelineBuilderInterface pipelineBuilderInterface;
    CacheIOInterface cacheIOInterface;

    @BeforeEach
    void init(){
        nodeFactory = new TransformNodeFactory();
        nodeTreeFactory = NodeTreeFactory.builder().nodeFactory(nodeFactory).build();
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File jsonFile = new File(classLoader.getResource(fileName).getFile());
        node = nodeTreeFactory.parseJsonFile(jsonFile.getAbsolutePath());

        cacheIOInterface = mock(CacheIOInterface.class);

    }

    @Test
    void checkClusterTraverser() {
        pipelineBuilderInterface = mock(PipelineBuilderInterface.class);

        doAnswer(invocation -> {
            Node node = invocation.getArgument(1);
            log.debug(node.getDescription());
            return null;
        }).when (pipelineBuilderInterface).buildPipeline(any(List.class), any(Node.class));

        AtomicInteger countCluster=new AtomicInteger();
        ClusterDependencyGraph.ClusterDependencyGraphBuilder clusterDependencyGraphBuilder = ClusterDependencyGraph.builder();
        clusterDependencyGraphBuilder.constructDependencyGraph(node);
        ClusterDependencyGraph clusterDependencyGraph = clusterDependencyGraphBuilder.build();

        List<Node> nodeList = null;
        int step = 0;
        while (true){
            int cntClusters = 0;
            nodeList = clusterDependencyGraph.findClusterWithoutDependency();
            if (nodeList.size()==0){
                break;
            }
            for (Node node: nodeList){
                countCluster.incrementAndGet();
                NodePipeline nodePipeline = GraphNodePipeline.builder()
                        .cluster(node.getCluster())
                        .pipelineBuilderInterface(pipelineBuilderInterface)
                        .cacheIOInterface(cacheIOInterface)
                        .build();
                List<Node> traversedResult = Lists.newLinkedList();
                NodeTraverser.postOrderTraversalExecution(node, null, traversedResult,nodePipeline );

                clusterDependencyGraph.removeClusterDependency(node);
                cntClusters++;
            }

            step++;
        }
        log.debug(String.format("Number of clusters: %d",countCluster.get()));
        assertEquals(4, countCluster.get());

    }

    @Test
    void checkCluster2Pipeline() {
        pipelineBuilderInterface = mock(PipelineBuilderInterface.class);

        doAnswer(invocation -> {
            List<Node> parentList = invocation.getArgument(0);
            Node node = invocation.getArgument(1);

            if (parentList.size()==0){
                log.debug(String.format("Insert node %s to root", node));
            }
            parentList.forEach(
                    p->{
                        log.debug(String.format("Insert node %s to parents %s", node, p));
                    }
            );

            return null;
        }).when (pipelineBuilderInterface).buildPipeline(any(List.class), any(Node.class));

        ClusterDependencyGraph.ClusterDependencyGraphBuilder clusterDependencyGraphBuilder = ClusterDependencyGraph.builder();
        clusterDependencyGraphBuilder.constructDependencyGraph(node);
        ClusterDependencyGraph clusterDependencyGraph = clusterDependencyGraphBuilder.build();

        List<Node> nodeList = null;
        int step = 0;
        int cntClusters = 0;
        while (true){

            nodeList = clusterDependencyGraph.findClusterWithoutDependency();
            if (nodeList.size()==0){
                break;
            }
            for (Node node: nodeList){

                NodePipeline nodePipeline = GraphNodePipeline.builder()
                        .cluster(node.getCluster())
                        .pipelineBuilderInterface(pipelineBuilderInterface)
                        .cacheIOInterface(cacheIOInterface)
                        .build();
                List<Node> traversedResult = Lists.newLinkedList();
                NodeTraverser.postOrderTraversalExecution(node, null, traversedResult,nodePipeline );
                nodePipeline.getPipelineBuilder();
                clusterDependencyGraph.removeClusterDependency(node);
                cntClusters++;
            }

            step++;
        }
        verify(pipelineBuilderInterface,times(3+1+3+3)).buildPipeline(anyList(),any(Node.class));
        assertEquals(3, step);
        assertEquals(4, cntClusters);
        verify(cacheIOInterface, times(3)).getRetrievedValue(any(String.class));
    }

}