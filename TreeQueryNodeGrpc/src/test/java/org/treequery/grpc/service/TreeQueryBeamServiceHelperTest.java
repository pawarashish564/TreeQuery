package org.treequery.grpc.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.grpc.utils.TestDataAgent;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.QueryTypeEnum;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.StatusTreeQueryCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

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
        TreeQueryBeamServiceHelper.ReturnResult returnResult = treeQueryBeamServiceHelper.process(TreeQueryRequest.RunMode.DIRECT,
                jsonString,
                true,
                pageSize,
                2);

        assertThat(returnResult.statusTreeQueryCluster.getStatus()).isEqualTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
        assertThat(returnResult.genericRecordList).hasSize(pageSize);
    }
}