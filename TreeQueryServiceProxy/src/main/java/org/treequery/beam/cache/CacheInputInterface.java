package org.treequery.beam.cache;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.model.CacheTypeEnum;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Consumer;

public interface CacheInputInterface {

    public Schema getPageRecordFromAvroCache(@Nullable Cluster cluster,
                                             String identifier,
                                             long pageSize,
                                             long page,
                                             Consumer<GenericRecord> dataConsumer,
                                             @Nullable Schema schema) throws CacheNotFoundException ;
    public void getStreamRecordFromAvroCache(@Nullable Cluster cluster,
                                             String identifier,
                                             Consumer<GenericRecord> dataConsumer,
                                             @Nullable Schema schema) throws CacheNotFoundException ;

    static Cluster getCluster(DiscoveryServiceInterface discoveryServiceInterface, Cluster cluster, String identifier){
        Cluster clusterStore = Optional.ofNullable(cluster).orElse(discoveryServiceInterface.getCacheResultCluster(identifier));
        clusterStore = Optional.ofNullable(clusterStore).orElseThrow(()->new IllegalStateException(
                String.format("No cache result for %s",identifier)));
        return clusterStore;
    }
}
