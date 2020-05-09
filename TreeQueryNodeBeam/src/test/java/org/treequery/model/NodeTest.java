package org.treequery.model;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.Transform.JoinNode;
import org.treequery.utils.AvroSchemaHelper;
import org.treequery.utils.JsonInstructionHelper;
import org.treequery.utils.TestDataAgent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

@Slf4j
class NodeTest {
    JoinNode joinNode = null;
    AvroSchemaHelper avroSchemaHelper = null;
    String originalJson;
    @BeforeEach
    void init() throws Exception{
        String AvroTree = "SimpleJoin.json";
        originalJson = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        Node rootNode = JsonInstructionHelper.createNode(originalJson);
        assertThat(rootNode).isInstanceOf(JoinNode.class);
        joinNode = (JoinNode)rootNode;
        avroSchemaHelper = mock(AvroSchemaHelper.class);
    }
    @Test
    void ConverttoJsonToandFrom() throws Exception {
        String orgIdentifier = joinNode.getIdentifier();
        String joinJsonStr = joinNode.toJson();
        Node recoveredNode = JsonInstructionHelper.createNode(joinJsonStr);
        assertThat(recoveredNode).isInstanceOf(JoinNode.class);
        String newIdentifier = recoveredNode.getIdentifier();
        assertEquals(orgIdentifier, newIdentifier);

        joinNode.getChildren().forEach(
                childNode->{
                    assertNotNull(childNode.getJNode());
                    String childJsonStr = childNode.toJson();
                    String orgChildIdentifier = childNode.getIdentifier();
                    try {
                        Node recoveredChild = JsonInstructionHelper.createNode(childJsonStr);
                        String newChildIdentifier = recoveredChild.getIdentifier();
                        assertEquals(orgChildIdentifier, newChildIdentifier);
                    }catch(Exception ex){
                        ex.printStackTrace();
                    }

                }
        );
    }
}