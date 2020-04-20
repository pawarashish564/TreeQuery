package org.treequery.beam.cache;

import com.google.common.collect.Maps;
import lombok.Builder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.model.Location;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.grpc.client.GrpcClientChannel;
import org.treequery.model.CacheTypeEnum;
import org.treequery.proto.TreeQueryCacheRequest;
import org.treequery.proto.TreeQueryCacheServiceGrpc;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.Consumer;

@Builder
public class TreeQueryCacheProxy implements CacheInputInterface {
    private volatile Map<Cluster, TreeQueryCacheServiceGrpc.TreeQueryCacheServiceBlockingStub> clusterGrpcClientChannelMap = Maps.newConcurrentMap();

    private final DiscoveryServiceInterface discoveryServiceInterface;

    private TreeQueryCacheServiceGrpc.TreeQueryCacheServiceBlockingStub getBlockingStub(Cluster cluster){
        clusterGrpcClientChannelMap.putIfAbsent(cluster, createNewBlockingStub(discoveryServiceInterface, cluster));
        return clusterGrpcClientChannelMap.get(cluster);
    }
    private static TreeQueryCacheServiceGrpc.TreeQueryCacheServiceBlockingStub createNewBlockingStub(DiscoveryServiceInterface discoveryServiceInterface, Cluster cluster){
        Location location =discoveryServiceInterface.getClusterLocation(cluster);
        GrpcClientChannel grpcClientChannel = new GrpcClientChannel(location.getAddress(), location.getPort());
        TreeQueryCacheServiceGrpc.TreeQueryCacheServiceBlockingStub treeQueryCacheServiceBlockingStub = TreeQueryCacheServiceGrpc.newBlockingStub(grpcClientChannel.getChannel());
        return treeQueryCacheServiceBlockingStub;
    }

    @Override
    public Schema getPageRecordFromAvroCache(@Nullable Cluster cluster, CacheTypeEnum cacheTypeEnum, String identifier, long pageSize, long page, Consumer<GenericRecord> dataConsumer, Schema schema) throws CacheNotFoundException {

        TreeQueryCacheServiceGrpc.TreeQueryCacheServiceBlockingStub treeQueryCacheServiceBlockingStub = this.getBlockingStub(cluster);

        String avroSchema="";
        TreeQueryCacheRequest.Builder treeQueryCacheRequestBuilder = TreeQueryCacheRequest.newBuilder();
        treeQueryCacheRequestBuilder.setIdentifier(identifier);
        treeQueryCacheRequestBuilder.setPage(page);
        treeQueryCacheRequestBuilder.setPageSize(pageSize);
        treeQueryCacheRequestBuilder.setAvroSchema(avroSchema);

        return null;
    }
}
