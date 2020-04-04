package org.treequery.beam.transform;

import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.model.Node;
import org.treequery.util.AvroIOHelper;
import org.treequery.util.JsonInstructionHelper;
import org.treequery.utils.TestDataAgent;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class JoinNodeHelperTest {

    @BeforeAll
    static void init(){

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


    }
}