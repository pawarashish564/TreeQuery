package org.treequery.beam.transform;

import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.Transform.JoinNode;
import org.treequery.Transform.function.JoinFunction;
import org.treequery.model.JoinAble;
import org.treequery.model.Node;
import org.treequery.util.AvroIOHelper;
import org.treequery.util.GenericRecordSchemaHelper;
import org.treequery.util.JsonInstructionHelper;
import org.treequery.utils.TestDataAgent;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class JoinNodeHelperTest {
    JoinNode joinNode = null;
    @BeforeEach
    void init() throws Exception{
        String AvroTree = "SimpleJoin.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        Node rootNode = JsonInstructionHelper.createNode(jsonString);
        assertThat(rootNode).isInstanceOf(JoinNode.class);
        joinNode = (JoinNode)rootNode;
    }

    List<GenericRecord> getAvroRecord(String fileName) throws Exception{
        List<GenericRecord> genericRecords = Lists.newLinkedList();
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File avroFile = new File(classLoader.getResource(fileName).getFile());
        AvroIOHelper.readAvroGenericRecordFile(avroFile,null,
                (record)->{
                    genericRecords.add(record);
                });
        return genericRecords;
    }

    @Test
    void giveCorrectKeyPerGenericRecordDuringJoin() throws Exception{
        JoinNodeHelper joinNodeHelper = new JoinNodeHelper();
        List<GenericRecord> bondTrades = getAvroRecord("bondtrade1.avro");
        List<GenericRecord> bondStatics = getAvroRecord("BondStaticSample.avro");
        assertThat(bondTrades).hasSizeGreaterThan(0);
        assertThat(bondStatics).hasSize(16);
        JoinFunction joinFunction = (JoinFunction) joinNode.getJoinFunction();
        List<JoinAble.JoinKey> joinKeyList = joinFunction.getJoinKeys();
        JoinAble.JoinKey joinKey = joinKeyList.get(0);
        List<String> bondTradeColumn = joinKey.getColumnStream().map(k->k.getLeftColumn()).collect(Collectors.toList());
        List<String> bondStaticColumn = joinKey.getColumnStream().map(k->k.getRightColumn()).collect(Collectors.toList());

        bondTrades.forEach(
                bondTrade->{
                    String key = JoinNodeHelper.getKeyStringHelper(bondTrade, bondTradeColumn);
                    String secid = GenericRecordSchemaHelper.StringifyAvroValue(bondTrade,"asset.securityId");
                    assertEquals(secid+"-", key);
                }
        );
        bondStatics.forEach(
                bondStatic ->{
                    String key = JoinNodeHelper.getKeyStringHelper(bondStatic, bondStaticColumn);
                    String secid = GenericRecordSchemaHelper.StringifyAvroValue(bondStatic, "isin_code");
                    assertEquals(secid+"-", key);
                }
        );

    }
}