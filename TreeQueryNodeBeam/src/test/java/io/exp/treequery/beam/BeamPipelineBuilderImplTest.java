package io.exp.treequery.beam;

import com.google.common.collect.Lists;
import io.exp.treequery.Transform.TransformNodeFactory;
import io.exp.treequery.cluster.ClusterDependencyGraph;
import io.exp.treequery.cluster.NodeFactory;
import io.exp.treequery.cluster.NodeTreeFactory;
import io.exp.treequery.execute.*;
import io.exp.treequery.model.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BeamPipelineBuilderImplTest {
    CacheInputInterface cacheInputInterface;
    String fileName = "bondtrade1.avro";


    NodeFactory nodeFactory;
    NodeTreeFactory nodeTreeFactory;

    @BeforeEach
    void init(){

        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File jsonFile = new File(classLoader.getResource(fileName).getFile());
        String directoryName = jsonFile.getParent();

        cacheInputInterface = FileCacheInputImpl.builder()
                                .fileDirectory(directoryName).build();

    }

    @Test
    void simpleAvroReadPipeline() {
        String simpleAvroTree = "SimpleAvroReadCluster.json";
        Node rootNode = null;
        nodeFactory = new TransformNodeFactory();
        nodeTreeFactory = NodeTreeFactory.builder().nodeFactory(nodeFactory).build();
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File jsonFile = new File(classLoader.getResource(simpleAvroTree).getFile());
        rootNode = nodeTreeFactory.parseJsonFile(jsonFile.getAbsolutePath());


        ClusterDependencyGraph.ClusterDependencyGraphBuilder clusterDependencyGraphBuilder = ClusterDependencyGraph.builder();
        clusterDependencyGraphBuilder.constructDependencyGraph(rootNode);
        ClusterDependencyGraph clusterDependencyGraph = clusterDependencyGraphBuilder.build();

        List<Node> nodeList = null;
        nodeList = clusterDependencyGraph.findClusterWithoutDependency();
        assertThat(nodeList).hasSize(1);

        for (Node node: nodeList){
            PipelineBuilderInterface pipelineBuilderInterface = BeamPipelineBuilderImpl.builder()
                    .cacheInputInterface(cacheInputInterface)
                    .build();
            NodePipeline nodePipeline = GraphNodePipeline.builder()
                    .cluster(node.getCluster())
                    .pipelineBuilderInterface(pipelineBuilderInterface)
                    .cacheInputInterface(cacheInputInterface)
                    .build();
            List<Node> traversedResult = Lists.newLinkedList();
            NodeTraverser.postOrderTraversalExecution(node, null, traversedResult,nodePipeline );
            nodePipeline.getPipelineBuilder();
            clusterDependencyGraph.removeClusterDependency(node);
        }

    }


}