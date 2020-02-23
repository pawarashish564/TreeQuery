package io.exp.treequery.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import io.exp.treequery.model.ActionTypeEnum;
import io.exp.treequery.model.Node;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.atLeastOnce;

//@ExtendWith(MockitoExtension.class)
@Slf4j
class NodeTreeFactoryTest {
    String fileName = "TreeQueryInput3.json";

    //@Mock
    NodeFactory nodeFactory;

    //@InjectMocks
    NodeTreeFactory nodeTreeFactory;

    @BeforeEach
    public void init(){
        nodeFactory = mock(NodeFactory.class);
        nodeTreeFactory = NodeTreeFactory.builder().nodeFactory(nodeFactory).build();
    }
    @Test
    void verifyNavigationJNodeWorks() {

        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());

        when (nodeFactory.nodeFactoryMethod(any(JsonNode.class))).then(
            invocation -> {
                JsonNode jsonNode = invocation.getArgument(0);
                DummyNode node = new DummyNode();
                node.setDescription(jsonNode.get("description").asText());
                node.setAction(ActionTypeEnum.valueOf(jsonNode.get("action").asText()));
                node.setCluster(Cluster.builder()
                        .clusterName(jsonNode.get("cluster").asText())
                        .build());
                return node;
            }
        );
        Node node = nodeTreeFactory.parseJsonFile(file.getAbsolutePath());
        log.debug(node.toString());
        Cluster clusterA = Cluster.builder().clusterName("A").build();
        Cluster clusterB = Cluster.builder().clusterName("B").build();
        Cluster clusterC = Cluster.builder().clusterName("C").build();
        assertAll(
                ()->{
                    verify(nodeFactory, times(7)).nodeFactoryMethod(any(JsonNode.class));
                },
                ()->{
                    assertEquals(ActionTypeEnum.FLATTEN,node.getAction());
                    assertThat(node.getChildren()).hasSize(2);
                    assertEquals(clusterC, node.getCluster());
                },
                ()->{
                    assertThat(node.getChildren().get(0).getAction()).isNotNull();
                    assertThat(node.getChildren().get(1).getAction()).isNotNull();
                },
                ()->{
                    Node cNode = node.getChildren().get(0);
                    assertEquals(ActionTypeEnum.INNER_JOIN, cNode.getAction());
                    assertEquals(clusterB, cNode.getCluster() );
                    assertThat(cNode.getChildren()).hasSize(2);
                    Node cQNode = cNode.getChildren().get(0);
                    assertEquals(ActionTypeEnum.QUERY,cQNode.getAction());
                    assertEquals(clusterB, cQNode.getCluster());
                    cQNode = cNode.getChildren().get(1);
                    assertEquals(ActionTypeEnum.LOAD,cQNode.getAction());
                    assertEquals(clusterA, cQNode.getCluster());
                },
                ()->{
                    Node cNode = node.getChildren().get(1);
                    assertEquals(ActionTypeEnum.INNER_JOIN, cNode.getAction());
                    assertEquals(clusterA, cNode.getCluster() );
                    assertThat(cNode.getChildren()).hasSize(2);
                    Node cQNode = cNode.getChildren().get(0);
                    assertEquals(ActionTypeEnum.QUERY,cQNode.getAction());
                    assertEquals(clusterA, cQNode.getCluster());
                    cQNode = cNode.getChildren().get(1);
                    assertEquals(ActionTypeEnum.LOAD,cQNode.getAction());
                    assertEquals(clusterA, cQNode.getCluster());
                }
        );
    }
}