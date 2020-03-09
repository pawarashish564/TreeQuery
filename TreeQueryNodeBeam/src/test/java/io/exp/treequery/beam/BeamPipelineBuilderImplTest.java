package io.exp.treequery.beam;

import io.exp.treequery.Transform.TransformNodeFactory;
import io.exp.treequery.cluster.ClusterDependencyGraph;
import io.exp.treequery.cluster.NodeFactory;
import io.exp.treequery.cluster.NodeTreeFactory;
import io.exp.treequery.execute.CacheInputInterface;
import io.exp.treequery.execute.PipelineBuilderInterface;
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
        Node node = null;
        nodeFactory = new TransformNodeFactory();
        nodeTreeFactory = NodeTreeFactory.builder().nodeFactory(nodeFactory).build();
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File jsonFile = new File(classLoader.getResource(simpleAvroTree).getFile());
        node = nodeTreeFactory.parseJsonFile(jsonFile.getAbsolutePath());

        PipelineBuilderInterface pipelineBuilderInterface = BeamPipelineBuilderImpl.builder()
                                                            .cacheInputInterface(cacheInputInterface)
                                                            .build();
        ClusterDependencyGraph.ClusterDependencyGraphBuilder clusterDependencyGraphBuilder = ClusterDependencyGraph.builder();
        clusterDependencyGraphBuilder.constructDependencyGraph(node);
        ClusterDependencyGraph clusterDependencyGraph = clusterDependencyGraphBuilder.build();

        List<Node> nodeList = null;
        nodeList = clusterDependencyGraph.findClusterWithoutDependency();
        assertThat(nodeList).hasSize(1);

    }


}