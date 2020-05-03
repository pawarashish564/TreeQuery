package org.treequery.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.treequery.Transform.QueryLeafNode;
import org.treequery.Transform.function.SqlQueryFunction;
import org.treequery.beam.cache.BeamCacheOutputBuilder;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

        treeQueryClusterService =  AsyncTreeQueryClusterService.builder()
                .treeQueryClusterRunnerFactory(()->{
                    return TreeQueryClusterRunnerImpl.builder()
                            .beamCacheOutputBuilder(BeamCacheOutputBuilder.builder()
                                    .treeQuerySetting(this.treeQuerySetting)
                                    .build())
                            .avroSchemaHelper(avroSchemaHelper)
                            .treeQuerySetting(treeQuerySetting)
                            .treeQueryClusterRunnerProxyInterface(treeQueryClusterRunnerProxyInterface)
                            .cacheInputInterface(cacheInputInterface)
                            .discoveryServiceInterface(discoveryServiceInterface)
                            .build();
                })
                .build();
        treeQueryClusterService.runQueryTreeNetwork(rootNode, (status)->{
            log.debug(status.toString());
            if (status.status == StatusTreeQueryCluster.QueryTypeEnum.FAIL){
                throw new IllegalStateException(status.description);
            }
            synchronized (rootNode) {
                rootNode.notify();
            }
            assertThat(status.status).isEqualTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
        });
        synchronized (rootNode){
            rootNode.wait();
        }
        //Check the avro file
        long pageSize = 10000;
        long page = 1;
        AtomicInteger counter = new AtomicInteger();
        Pattern tenorPattern = Pattern.compile("(\\d+)Y");
        Pattern datePattern = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2})");
        Schema schema = AvroIOHelper.getPageRecordFromAvroCache(
                treeQuerySetting,
                rootNode.getIdentifier(),pageSize,page,
                (record)->{
                    assertThat(record).isNotNull();
                    counter.incrementAndGet();
                    assertThat(record.get("AsOfDate")).isNotNull();
                    final GenericRecordSchemaHelper.StringField dateField = new GenericRecordSchemaHelper.StringField();
                    GenericRecordSchemaHelper.getValue(record, "AsOfDate", dateField);
                    assertThat(dateField.getValue()).matches((date)->{
                        Matcher matcher = datePattern.matcher(date);
                        return matcher.matches();
                    });

                    final GenericRecordSchemaHelper.StringField tenorField = new GenericRecordSchemaHelper.StringField();
                    GenericRecordSchemaHelper.getValue(record, "Tenor", tenorField);
                    assertThat(tenorField.getValue()).matches((tenor)->{
                        Matcher matcher = tenorPattern.matcher(tenor);
                        return matcher.matches();
                    });
                    final GenericRecordSchemaHelper.DoubleField priceField = new GenericRecordSchemaHelper.DoubleField();
                    GenericRecordSchemaHelper.getValue(record, "Price", priceField);
                    assertNotEquals(0.0,priceField.getValue());

                });

        assertEquals(4, counter.get());
    }
}
