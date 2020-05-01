package org.treequery.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.treequery.Transform.QueryLeafNode;
import org.treequery.Transform.function.SqlQueryFunction;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.*;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Slf4j
@Tag("integration")
public class SimpleAsyncOneNodeSqlServiceTest {
    TreeQueryClusterService treeQueryClusterService = null;
    AvroSchemaHelper avroSchemaHelper = null;
    BeamCacheOutputInterface beamCacheOutputInterface = null;
    DiscoveryServiceInterface discoveryServiceInterface = null;
    TreeQuerySetting treeQuerySetting = null;
    TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;
    CacheInputInterface cacheInputInterface;

    @BeforeEach
    void init() throws IOException {
        treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
        avroSchemaHelper = new BasicAvroSchemaHelperImpl();
        beamCacheOutputInterface = new TestFileBeamCacheOutputImpl();
        discoveryServiceInterface = mock(DiscoveryServiceInterface.class);
        treeQueryClusterRunnerProxyInterface = mock(TreeQueryClusterRunnerProxyInterface.class);
        cacheInputInterface = mock(CacheInputInterface.class);
    }

    @Test
    void runAsyncSimpleSQLReadTesting() throws Exception{
        String AvroTree = "SimpleSQLReadCluster.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        Node rootNode = JsonInstructionHelper.createNode(jsonString);
        assertThat(rootNode).isInstanceOf(QueryLeafNode.class);
        QueryLeafNode queryLeafNode = (QueryLeafNode)rootNode;

        assertThat(queryLeafNode.getQueryAble()).isInstanceOf(SqlQueryFunction.class);
        SqlQueryFunction sqlQueryFunction = (SqlQueryFunction)queryLeafNode.getQueryAble();
        assertThat(sqlQueryFunction.getDriverClassName()).isNotBlank();

    }
}
