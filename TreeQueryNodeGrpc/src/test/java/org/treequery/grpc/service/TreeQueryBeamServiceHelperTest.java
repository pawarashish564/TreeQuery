package org.treequery.grpc.service;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.grpc.utils.SettingInitializer;
import org.treequery.grpc.utils.TestDataAgent;
import org.treequery.utils.BasicAvroSchemaHelperImpl;
import org.treequery.model.CacheTypeEnum;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.StatusTreeQueryCluster;
import org.treequery.utils.AvroSchemaHelper;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
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
        DataConsumer2LinkedList genericRecordConsumer = new DataConsumer2LinkedList();
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

    @Test
    void CheckGetFromCacheRecord() {
        int pageSize = 100;
        DataConsumer2Set genericRecordConsumer = new DataConsumer2Set();
        TreeQueryBeamServiceHelper.PreprocessInput preprocessInput = treeQueryBeamServiceHelper.preprocess(jsonString);

        TreeQueryBeamServiceHelper.ReturnResult returnResult = treeQueryBeamServiceHelper.process(TreeQueryRequest.RunMode.DIRECT,
                preprocessInput,
                true,
                pageSize,
                2,
                genericRecordConsumer);

        assertThat(returnResult.statusTreeQueryCluster.getStatus()).isEqualTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
        assertThat(genericRecordConsumer.getGenericRecordSet()).hasSize(pageSize);
        genericRecordConsumer.getGenericRecordSet().forEach(
                genericRecord -> {
                    //log.debug(genericRecord.toString());
                    assertThat(genericRecord.toString()).isNotBlank();
                }
        );
        DataConsumer2Set cachedRecordConsumer = new DataConsumer2Set();
        TreeQueryBeamServiceHelper.ReturnResult returnResult2 = treeQueryBeamServiceHelper.process(TreeQueryRequest.RunMode.DIRECT,
                preprocessInput,
                false,
                pageSize,
                2,
                cachedRecordConsumer);
        assertThat(returnResult2.statusTreeQueryCluster.getStatus()).isEqualTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
        assertThat(cachedRecordConsumer.getGenericRecordSet()).hasSize(pageSize);
        assertThat(returnResult2.statusTreeQueryCluster.getDescription()).isEqualTo("Fresh from cache");
        genericRecordConsumer.getGenericRecordSet().forEach(
                genericRecord -> {
                    assertThat(cachedRecordConsumer.getGenericRecordSet()).contains(genericRecord);
                }
        );
    }

    private static class DataConsumer2LinkedList implements Consumer<GenericRecord>{
        @Getter
        List<GenericRecord> genericRecordList = Lists.newLinkedList();
        @Override
        public void accept(GenericRecord genericRecord) {
            genericRecordList.add(genericRecord);
        }
    }
    private static class DataConsumer2Set implements Consumer<GenericRecord>{
        @Getter
        Set<GenericRecord> genericRecordSet = Sets.newHashSet();
        @Override
        public void accept(GenericRecord genericRecord) {
            genericRecordSet.add(genericRecord);
        }
    }
}