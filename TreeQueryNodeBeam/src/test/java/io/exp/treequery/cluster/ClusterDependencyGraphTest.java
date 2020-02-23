package io.exp.treequery.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import io.exp.treequery.model.ActionTypeEnum;
import io.exp.treequery.model.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

class ClusterDependencyGraphTest {
    String fileName = "TreeQueryInput3.json";
    NodeFactory nodeFactory;
    NodeTreeFactory nodeTreeFactory;

    Node node = null;
    @BeforeEach
    void init(){
        nodeFactory = mock(NodeFactory.class);
        nodeTreeFactory = NodeTreeFactory.builder().nodeFactory(nodeFactory).build();
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());

        DummyNode.createMockBehavior(nodeFactory);
        node = nodeTreeFactory.parseJsonFile(file.getAbsolutePath());
    }
    @Test
    void VerifyBuilder() {
        assertThat(node).isNotNull();
        verify(nodeFactory, times(7)).nodeFactoryMethod(any(JsonNode.class));

        ClusterDependencyGraph.ClusterDependencyGraphBuilder clusterDependencyGraphBuilder = ClusterDependencyGraph.builder();
        assertTimeout(Duration.ofMillis(100), () -> {
            clusterDependencyGraphBuilder.constructDependencyGraph(node);
        });

        ClusterDependencyGraph clusterDependencyGraph = clusterDependencyGraphBuilder.build();

        assertThat(clusterDependencyGraph.clusterDepGraph.size()).isEqualTo(4);
        Map<Node, Set> clusterDepGraph = clusterDependencyGraph.clusterDepGraph;
        Map<Node, Node> cacheDependency = clusterDependencyGraph.cacheDependency;
        assertAll(
                ()->{
                    clusterDepGraph.keySet().forEach(
                            node->{
                                if (node.getDescription().equals("Load BondTrades 10Y")){
                                    assertThat(clusterDepGraph.get(node)).hasSize(0);
                                }else if(node.getDescription().equals("Join 5Y data")){
                                    assertThat(clusterDepGraph.get(node)).hasSize(0);
                                }else if(node.getDescription().equals("Join 10Y data")){
                                    assertThat(clusterDepGraph.get(node)).hasSize(1);
                                }else if(node.getDescription().equals("Flatten 5Y+10Y data")){
                                    assertThat(clusterDepGraph.get(node)).hasSize(2);
                                }
                            }
                    );
                },
                ()->{
                    assertThat(cacheDependency).hasSize(4);
                    cacheDependency.entrySet().forEach(
                            entry->{
                                Node node = entry.getKey();
                                Node parentNode = entry.getValue();
                                if (parentNode!=null) {
                                    assertThat(clusterDepGraph.get(parentNode)).isNotNull();
                                    assertThat(clusterDepGraph.get(parentNode)).contains(node);
                                }
                            }
                    );
                }
        );
    }

    @Test
    void verifyInitialClusterWithoutDependency() {
        ClusterDependencyGraph.ClusterDependencyGraphBuilder clusterDependencyGraphBuilder = ClusterDependencyGraph.builder();
        clusterDependencyGraphBuilder.constructDependencyGraph(node);
        ClusterDependencyGraph clusterDependencyGraph = clusterDependencyGraphBuilder.build();
        List<Node> nodeList = clusterDependencyGraph.findClusterWithoutDependency();
        assertThat(nodeList).hasSize(2);
    }


    @Test
    void verifyFirstRemoveClusterDependency() {
        ClusterDependencyGraph.ClusterDependencyGraphBuilder clusterDependencyGraphBuilder = ClusterDependencyGraph.builder();
        clusterDependencyGraphBuilder.constructDependencyGraph(node);
        ClusterDependencyGraph clusterDependencyGraph = clusterDependencyGraphBuilder.build();
        List<Node> nodeList = clusterDependencyGraph.findClusterWithoutDependency();

        Supplier<Stream<String>> streamSupplier
                = () -> nodeList.stream().map(node->node.getDescription());

        assertThat(streamSupplier.get().distinct().count()).isEqualTo(2);
        assertTrue(streamSupplier.get().allMatch(
                des -> des.equals("Load BondTrades 10Y") || des.equals("Join 5Y data")
        ));

    }
}