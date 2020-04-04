package org.treequery.model;

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.util.JsonInstructionHelper;
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
        assertEquals(jsonStr,"{\"fields\":[{\"name\":\"bondtrade\",\"type\":{\"name\":\"BondStatic\",\"type\":\"record\",\"namespace\":\"io.exp.security.model.avro\",\"fields\":[{\"name\":\"expected_maturity_date\",\"type\":\"string\"},{\"name\":\"original_maturity\",\"type\":\"string\"},{\"name\":\"issue_number\",\"type\":\"string\"},{\"name\":\"isin_code\",\"type\":\"string\"},{\"name\":\"coupon\",\"type\":\"string\"},{\"name\":\"outstanding_size\",\"type\":\"double\"},{\"name\":\"institutional_retail\",\"type\":\"string\"}]}},{\"name\":\"bondstatic\",\"type\":{\"name\":\"BondTrade\",\"type\":\"record\",\"namespace\":\"io.exp.security.model.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"cust\",\"type\":\"string\"},{\"name\":\"tradeDate\",\"type\":\"string\"},{\"name\":\"tradeType\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\",\"logicalType\":\"time-millis\"},{\"name\":\"asset\",\"type\":{\"name\":\"Asset\",\"type\":\"record\",\"fields\":[{\"name\":\"securityId\",\"type\":\"string\"},{\"name\":\"notional\",\"type\":\"double\"},{\"name\":\"price\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\"},{\"name\":\"bidask\",\"type\":{\"name\":\"BidAsk\",\"type\":\"enum\",\"symbols\":[\"BID\",\"ASK\"]}}]}}]}}],\"name\":\"Join5YData\",\"type\":\"record\",\"namespace\":\"org.treequery.join\"}");
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