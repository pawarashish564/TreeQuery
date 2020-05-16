package org.treequery.grpc.service;

import lombok.Builder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.treequery.config.TreeQuerySetting;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.model.CacheTypeEnum;
import org.treequery.service.CacheResult;
import org.treequery.utils.AvroIOHelper;

import java.util.function.Consumer;

@Builder
public class TreeQueryCacheServiceHelper implements TreeQueryCacheService{
    private final TreeQuerySetting treeQuerySetting;

    @Override
    public CacheResult get(String identifier, long pageSize, long page, Consumer<GenericRecord> dataConsumer) {
        Schema schema = null;
        CacheResult.CacheResultBuilder cacheResultBuilder = CacheResult.builder();
        cacheResultBuilder.identifier(identifier);
        cacheResultBuilder.queryTypeEnum(CacheResult.QueryTypeEnum.RUNNING);
        try {
            schema = AvroIOHelper.getPageRecordFromAvroCache(
                    this.treeQuerySetting,
                    identifier, pageSize, page, dataConsumer);
            cacheResultBuilder.dataSchema(schema);
            cacheResultBuilder.queryTypeEnum(CacheResult.QueryTypeEnum.SUCCESS);
            cacheResultBuilder.description("OK");
        }catch(CacheNotFoundException che){
            cacheResultBuilder.description(che.getMessage());
            cacheResultBuilder.queryTypeEnum(CacheResult.QueryTypeEnum.NOTFOUND);
        }catch(NoSuchMethodError ne){
            cacheResultBuilder.description(ne.getMessage());
            cacheResultBuilder.queryTypeEnum(CacheResult.QueryTypeEnum.FAIL);
        }
        catch(Throwable ex){
            cacheResultBuilder.description(ex.getMessage());
            cacheResultBuilder.queryTypeEnum(CacheResult.QueryTypeEnum.SYSTEMERROR);
        }
        return cacheResultBuilder.build();
    }

    @Override
    public Schema getSchemaOnly(String identifier) {
        Schema schema = null;
        schema = AvroIOHelper.getSchemaFromAvroCache(treeQuerySetting, identifier);
        return schema;
    }
}
