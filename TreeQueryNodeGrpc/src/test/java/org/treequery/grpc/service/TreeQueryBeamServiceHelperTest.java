package org.treequery.grpc.service;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.grpc.utils.TestDataAgent;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.QueryTypeEnum;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.StatusTreeQueryCluster;

import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
@Slf4j
class TreeQueryBeamServiceHelperTest {
    String jsonString;
    TreeQueryBeamServiceHelper treeQueryBeamServiceHelper;
    @BeforeEach
    void init(){
        String AvroTree = "SimpleJoin.json";
        jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        treeQueryBeamServiceHelper = new TreeQueryBeamServiceHelper(CacheTypeEnum.FILE);
    }
    @Test
    void happyPathRunBeamJoinLocally() {
        //TreeQueryRequest treeQueryRequest =  TreeQueryRequest.
        int pageSize = 3;
        DataConsumer genericRecordConsumer = new DataConsumer();
        TreeQueryBeamServiceHelper.PreprocessInput preprocessInput = treeQueryBeamServiceHelper.preprocess(jsonString);

        TreeQueryBeamServiceHelper.ReturnResult returnResult = treeQueryBeamServiceHelper.process(TreeQueryRequest.RunMode.DIRECT,
                preprocessInput,
                true,
                pageSize,
                2,
                genericRecordConsumer);

        assertThat(returnResult.statusTreeQueryCluster.getStatus()).isEqualTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
        assertThat(genericRecordConsumer.getGenericRecordList()).hasSize(pageSize);
        genericRecordConsumer.getGenericRecordList().forEach(
                genericRecord -> {
                    //log.debug(genericRecord.toString());
                    assertThat(genericRecord.toString()).isNotBlank();
                }
        );

    }

    private static class DataConsumer implements Consumer<GenericRecord>{
        @Getter
        List<GenericRecord> genericRecordList = Lists.newLinkedList();
        @Override
        public void accept(GenericRecord genericRecord) {
            genericRecordList.add(genericRecord);
        }
    }
}