package org.treequery.grpc.service;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.grpc.utils.SettingInitializer;
import org.treequery.grpc.utils.TestDataAgent;
import org.treequery.model.BasicAvroSchemaHelperImpl;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.QueryTypeEnum;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.StatusTreeQueryCluster;
import org.treequery.utils.AvroSchemaHelper;

import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

@Slf4j
class TreeQueryBeamServiceHelperTest {
    String jsonString;
    TreeQueryBeamServiceHelper treeQueryBeamServiceHelper;
    DiscoveryServiceInterface discoveryServiceInterface;
    AvroSchemaHelper avroSchemaHelper;
    TreeQuerySetting treeQuerySetting;
    @BeforeEach
    void init(){
        String AvroTree = "SimpleJoin.json";
        treeQuerySetting = SettingInitializer.createTreeQuerySetting();

        jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();
        discoveryServiceInterface = mock(DiscoveryServiceInterface.class);
        treeQueryBeamServiceHelper = TreeQueryBeamServiceHelper.builder()
                .cacheTypeEnum(CacheTypeEnum.FILE)
                .avroSchemaHelper(avroSchemaHelper)
                .discoveryServiceInterface(discoveryServiceInterface)
                .treeQuerySetting(treeQuerySetting)
                .build();
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