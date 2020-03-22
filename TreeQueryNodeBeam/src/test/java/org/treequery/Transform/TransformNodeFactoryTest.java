package org.treequery.Transform;

import org.treequery.cluster.Cluster;
import org.treequery.cluster.NodeFactory;
import org.treequery.cluster.NodeTreeFactory;
import org.treequery.model.ActionTypeEnum;
import org.treequery.model.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class TransformNodeFactoryTest {

    String fileName = "TreeQueryInput3.json";
    NodeFactory nodeFactory;
    NodeTreeFactory nodeTreeFactory;
    Node node = null;

    Cluster clusterA = Cluster.builder().clusterName("A").build();
    Cluster clusterB = Cluster.builder().clusterName("B").build();
    Cluster clusterC = Cluster.builder().clusterName("C").build();

    @BeforeEach
    void init(){
        nodeFactory = new TransformNodeFactory();
        nodeTreeFactory = NodeTreeFactory.builder().nodeFactory(nodeFactory).build();
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File jsonFile = new File(classLoader.getResource(fileName).getFile());
        node = nodeTreeFactory.parseJsonFile(jsonFile.getAbsolutePath());
    }
    @Test
    void checkFlattenNode() {
        assertThat(node).isNotNull();
        assertThat(node.getDescription()).isEqualTo("Flatten 5Y+10Y data");
        assertThat(node.getAction()).isEqualTo(ActionTypeEnum.FLATTEN);
        assertThat(node.getChildren()).hasSize(2);
        assertThat(node.getIdentifier()).isNotBlank();
    }

    @Test
    void CheckInnerJoin() {
        List<Node> innerJoin = node.getChildren();
        Node Join5Ydata = innerJoin.get(1);
        assertThat(Join5Ydata.getChildren()).hasSize(2);
        assertEquals("Join 5Y data",Join5Ydata.getDescription());
        assertEquals(clusterA, Join5Ydata.getCluster());
        Node Join10Ydata = innerJoin.get(0);
        assertThat(Join10Ydata.getChildren()).hasSize(2);
        assertEquals("Join 10Y data",Join10Ydata.getDescription());
        assertEquals(clusterB, Join10Ydata.getCluster());
    }
}