package io.exp.treequery.beam;

import com.google.common.collect.Lists;
import io.exp.treequery.Transform.TransformNodeFactory;
import io.exp.treequery.cluster.ClusterDependencyGraph;
import io.exp.treequery.cluster.NodeFactory;
import io.exp.treequery.cluster.NodeTreeFactory;
import io.exp.treequery.execute.*;
import io.exp.treequery.model.Node;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
@Slf4j
class BeamPipelineBuilderImplTest {
    CacheInputInterface cacheInputInterface;
    String fileName = "bondtrade1.avro";

    String workDirectory = null;
    NodeFactory nodeFactory;
    NodeTreeFactory nodeTreeFactory;

    @BeforeEach
    void init(){

        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File jsonFile = new File(classLoader.getResource(fileName).getFile());
        workDirectory = jsonFile.getParent();

        cacheInputInterface = FileCacheInputImpl.builder()
                                .fileDirectory(workDirectory).build();

    }

    @Test
    void simpleAvroReadPipeline() throws Exception {
        String simpleAvroTree = "SimpleAvroReadCluster.json";
        Node rootNode = null;
        nodeFactory = new TransformNodeFactory();
        nodeTreeFactory = NodeTreeFactory.builder().nodeFactory(nodeFactory).build();
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File jsonFile = new File(classLoader.getResource(simpleAvroTree).getFile());

        String jsonString = this.parseJsonFile(jsonFile.getAbsolutePath());
        jsonString = jsonString.replaceAll("\\$\\{WORKDIR\\}", workDirectory);
        rootNode = nodeTreeFactory.parseJsonString(jsonString);


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
            Pipeline pipeline = pipelineBuilderInterface.getPipeline();
            pipeline.run();
            clusterDependencyGraph.removeClusterDependency(node);
        }

    }

    private  String parseJsonFile (String jsonFile)  {
        String jsonString="";
        StringBuilder contentBuilder = new StringBuilder();
        try(Stream<String> stream = Files.lines( Paths.get(jsonFile), StandardCharsets.UTF_8)){
            stream.forEach(s -> contentBuilder.append(s));
             jsonString= contentBuilder.toString();
        }catch(IOException ioe){
            log.error(ioe.getMessage());
        }
        return jsonString;
    }

}