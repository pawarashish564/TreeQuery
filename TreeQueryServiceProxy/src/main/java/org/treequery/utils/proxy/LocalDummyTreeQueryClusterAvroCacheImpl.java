package org.treequery.utils.proxy;

import jdk.internal.jline.internal.Nullable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.model.CacheTypeEnum;
import org.treequery.utils.AvroIOHelper;

import java.util.Optional;
import java.util.function.Consumer;

@Slf4j
@Builder
public class LocalDummyTreeQueryClusterAvroCacheImpl implements TreeQueryClusterAvroCacheInterface {

    private final TreeQuerySetting treeQuerySetting;
    private final DiscoveryServiceInterface discoveryServiceInterface;

    @Override
    public Schema getPageRecordFromAvroCache(@Nullable Cluster cluster, CacheTypeEnum cacheTypeEnum, String identifier, long pageSize, long page, Consumer<GenericRecord> dataConsumer) throws CacheNotFoundException {
        Cluster clusterStore = Optional.ofNullable(cluster).orElse(discoveryServiceInterface.getCacheResultCluster(identifier));
        clusterStore = Optional.ofNullable(clusterStore).orElseThrow(()->new IllegalStateException(
                String.format("No cache result for %s",identifier)));
        log.debug("Retrieve record from cluster:", clusterStore.toString());
        return AvroIOHelper.getPageRecordFromAvroCache(cacheTypeEnum, treeQuerySetting, identifier, pageSize, page, dataConsumer);
    }
}