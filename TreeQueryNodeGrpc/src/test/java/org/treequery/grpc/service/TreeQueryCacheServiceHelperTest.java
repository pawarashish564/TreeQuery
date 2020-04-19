package org.treequery.grpc.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.config.TreeQuerySetting;
import org.treequery.grpc.utils.TestDataAgent;
import org.treequery.model.CacheTypeEnum;
import org.treequery.service.CacheResult;
import org.treequery.utils.TreeQuerySettingHelper;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class TreeQueryCacheServiceHelperTest {
    CacheTypeEnum cacheTypeEnum;
    TreeQuerySetting treeQuerySetting;
    TreeQueryCacheService treeQueryCacheService;
    String identifier = "BondTradeJoinBondStatic";
    String avroSampleFile = identifier+".avro";
    String HOSTNAME = "localhost";
    int PORT = 9001;
    @BeforeEach
    void init(){

        cacheTypeEnum = CacheTypeEnum.FILE;
        TreeQuerySetting.TreeQuerySettingBuilder treeQuerySettingBuilder = new TreeQuerySetting.TreeQuerySettingBuilder(
            "A",
            HOSTNAME,
                PORT,
                TestDataAgent.getTestResourceDirectory(avroSampleFile),
                "",0
        );
        treeQuerySetting = treeQuerySettingBuilder.build();

        treeQueryCacheService = TreeQueryCacheServiceHelper.builder()
                                    .cacheTypeEnum(cacheTypeEnum)
                                    .treeQuerySetting(treeQuerySetting)
                                    .build();
    }

    @Test
    void happyPathToGetCacheResult() {
        AtomicInteger counter = new AtomicInteger(0);
        CacheResult cacheResult = treeQueryCacheService.get(identifier, 1 , 1, (record)->{
            counter.incrementAndGet();
            assertNotNull(record);
        });
        assertThat(counter).hasValueGreaterThan(0);
    }
}