package org.treequery.utils.proxy;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.model.CacheTypeEnum;
import org.treequery.utils.AvroIOHelper;

import javax.annotation.Nullable;
import java.util.function.Consumer;

@Slf4j
@Builder
public class LocalDummyCacheInputImpl implements CacheInputInterface {

    private final TreeQuerySetting treeQuerySetting;

    @Override
    public void getStreamRecordFromAvroCache(@Nullable Cluster cluster, String identifier, Consumer<GenericRecord> dataConsumer, @Nullable Schema schema) throws CacheNotFoundException {
        //throw new NoSuchMethodError("Not implemented");
        AvroIOHelper.getStreamRecordFromAvroCache(
                treeQuerySetting, identifier, dataConsumer,
                (throwable)->{}
        );
    }

    private final DiscoveryServiceInterface discoveryServiceInterface;

    @Override
    public Schema getPageRecordFromAvroCache(@Nullable Cluster cluster, String identifier, long pageSize, long page, Consumer<GenericRecord> dataConsumer, @Nullable Schema shema) throws CacheNotFoundException {
        Cluster clusterStore = CacheInputInterface.getCluster(discoveryServiceInterface, cluster, identifier);
        log.debug("Retrieve record from cluster:", clusterStore.toString());
        return AvroIOHelper.getPageRecordFromAvroCache(treeQuerySetting, identifier, pageSize, page, dataConsumer);
    }

    @Override
    public Schema getSchema(@Nullable Cluster cluster, String identifier) {
        return AvroIOHelper.getSchemaFromAvroCache(treeQuerySetting, identifier);
    }
}
