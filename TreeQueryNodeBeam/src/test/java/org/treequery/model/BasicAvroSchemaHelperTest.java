package org.treequery.model;

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.utils.JsonInstructionHelper;
import org.treequery.utils.TestDataAgent;

import static org.junit.jupiter.api.Assertions.*;

class BasicAvroSchemaHelperTest {
    AvroSchemaHelper avroSchemaHelper;

    @BeforeEach
    void init() {
        avroSchemaHelper = new BasicAvroSchemaHelper();
    }

    @Test
    void getSimpleJoinAvroSchemaString() throws Exception {
        String AvroTree = "SimpleJoin.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        Node rootNode = JsonInstructionHelper.createNode(jsonString);

        String jsonStr = avroSchemaHelper.getAvroSchemaJsonString(rootNode);
        assertNotNull(jsonStr);
    }

    @Test
    void getSimpleJoinAvroSchema() throws Exception {
        String AvroTree = "SimpleJoin.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        Node rootNode = JsonInstructionHelper.createNode(jsonString);

        Schema joinSchema =  avroSchemaHelper.getAvroSchema(rootNode);
        assertNotNull(joinSchema);
    }
}