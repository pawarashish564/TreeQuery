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
        CacheResult cacheResult = null;
        int pageSize = 1000;
        int page = 1;
        do {
            int inx = counter.get();
            cacheResult = treeQueryCacheService.get(identifier, pageSize , page, (record)->{
                counter.incrementAndGet();
                assertNotNull(record);
            });
            page+=1;
            if (inx == counter.get()){
                break;
            }
        }while(cacheResult.getQueryTypeEnum() == CacheResult.QueryTypeEnum.SUCCESS);

        assertThat(counter).hasValue(1000);
    }

    @Test
    void NotFoundGetCacheResult() {
        AtomicInteger counter = new AtomicInteger(0);
        CacheResult cacheResult = null;
        int pageSize = 1000;
        int page = 1;

        cacheResult = treeQueryCacheService.get("XXX", pageSize , page, (record)->{
                counter.incrementAndGet();
                assertNotNull(record);});


        assertThat(cacheResult.getQueryTypeEnum()).isEqualTo(CacheResult.QueryTypeEnum.NOTFOUND);
    }
}