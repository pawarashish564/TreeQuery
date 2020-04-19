package org.treequery.grpc.utils.proxy;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.model.Location;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.model.CacheTypeEnum;

import javax.annotation.Nullable;
import java.util.function.Consumer;

@Slf4j
@Builder
public class GrpcCacheInputImpl implements CacheInputInterface {
    @NonNull
    private final TreeQuerySetting treeQuerySetting;
    @NonNull
    private final DiscoveryServiceInterface discoveryServiceInterface;

    @Override
    public Schema getPageRecordFromAvroCache(@Nullable Cluster cluster, CacheTypeEnum cacheTypeEnum, String identifier, long pageSize, long page, Consumer<GenericRecord> dataConsumer) throws CacheNotFoundException {
        Cluster clusterStore = CacheInputInterface.getCluster(discoveryServiceInterface, cluster, identifier);
        log.debug("Retrieve record from cluster:", clusterStore.toString());

        Location location = discoveryServiceInterface.getClusterLocation(clusterStore);


        return null;
    }
}
