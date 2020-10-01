package org.treequery.Flow;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.Transform.JoinNode;
import org.treequery.dto.TreeNodeEvent;
import org.treequery.model.Node;
import org.treequery.utils.JsonInstructionHelper;
import org.treequery.utils.TestDataAgent;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class TreeNodeEventSubscriberTest {
    Node node;
    private List< TreeNodeEvent > refTreeNodeEventList;
    @BeforeEach
    void initTreeNode() throws Exception{
        String AvroTree = "TreeQueryInput4.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        node = JsonInstructionHelper.createNode(jsonString);
        assertThat(node).isInstanceOf(JoinNode.class);

        refTreeNodeEventList = Lists.newArrayList();
        refTreeNodeEventList.add(TreeNodeEvent.builder()
                .calcNode(node)
                .id(UUID.randomUUID().toString())
                .build());

    }

    @Test
    void onSubscribe() {
    }
}