package org.treequery.beam.cache;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import lombok.Builder;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.model.Location;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.grpc.client.GrpcClientChannel;
import org.treequery.grpc.utils.GenericRecordReader;
import org.treequery.model.CacheTypeEnum;
import org.treequery.proto.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class TreeQueryCacheProxy implements CacheInputInterface {
    private volatile Map<Cluster, TreeQueryCacheServiceGrpc.TreeQueryCacheServiceBlockingStub> clusterGrpcClientChannelMap = Maps.newConcurrentMap();
    @NonNull
    private final DiscoveryServiceInterface discoveryServiceInterface;

    @Builder
    TreeQueryCacheProxy(DiscoveryServiceInterface discoveryServiceInterface){
        this.discoveryServiceInterface = discoveryServiceInterface;
    }

    private TreeQueryCacheServiceGrpc.TreeQueryCacheServiceBlockingStub getBlockingStub(Cluster cluster)
            throws CacheNotFoundException{
        clusterGrpcClientChannelMap.putIfAbsent(cluster, createNewBlockingStub(discoveryServiceInterface, cluster));
        return clusterGrpcClientChannelMap.get(cluster);
    }
    private static TreeQueryCacheServiceGrpc.TreeQueryCacheServiceBlockingStub createNewBlockingStub
            (DiscoveryServiceInterface discoveryServiceInterface, Cluster cluster)
            throws CacheNotFoundException{
        Location location =Optional.ofNullable(discoveryServiceInterface.getClusterLocation(cluster))
                            .orElseThrow(()->new CacheNotFoundException("Cannot find location from " + cluster.toString()));
        GrpcClientChannel grpcClientChannel = new GrpcClientChannel(location.getAddress(), location.getPort());
        TreeQueryCacheServiceGrpc.TreeQueryCacheServiceBlockingStub treeQueryCacheServiceBlockingStub =
                TreeQueryCacheServiceGrpc.newBlockingStub(grpcClientChannel.getChannel());
        return treeQueryCacheServiceBlockingStub;
    }

    @Override
    public Schema getPageRecordFromAvroCache(@Nullable Cluster cluster,
                                             CacheTypeEnum cacheTypeEnum,
                                             String identifier,
                                             long pageSize,
                                             long page,
                                             Consumer<GenericRecord> dataConsumer,
                                             @Nullable Schema schema) throws CacheNotFoundException {

        Cluster _cluster = Optional.ofNullable(cluster).orElse(discoveryServiceInterface.getCacheResultCluster(identifier));
        Optional.ofNullable(_cluster).orElseThrow(()->new CacheNotFoundException("No cluster for "+identifier));
        TreeQueryCacheServiceGrpc.TreeQueryCacheServiceBlockingStub treeQueryCacheServiceBlockingStub = this.getBlockingStub(_cluster);

        String avroSchema= Optional.ofNullable(schema).map(Schema::toString).orElse("");
        TreeQueryCacheRequest.Builder treeQueryCacheRequestBuilder = TreeQueryCacheRequest.newBuilder();
        treeQueryCacheRequestBuilder.setIdentifier(identifier);
        treeQueryCacheRequestBuilder.setPage(page);
        treeQueryCacheRequestBuilder.setPageSize(pageSize);
        treeQueryCacheRequestBuilder.setAvroSchema(avroSchema);

        TreeQueryCacheResponse treeQueryCacheResponse = treeQueryCacheServiceBlockingStub.get(treeQueryCacheRequestBuilder.build());

        boolean success = treeQueryCacheResponse.getHeader().getSuccess();
        TreeQueryResponseHeader header = treeQueryCacheResponse.getHeader();
        if (!success){
            throw new CacheNotFoundException(String.format( "%d:%s",header.getErrCode(), header.getErrMsg()));
        }
        TreeQueryResponseResult result = treeQueryCacheResponse.getResult();
        Schema outputSchema = new Schema.Parser().parse(result.getAvroSchema());
        ByteString avroLoad = result.getAvroLoad();
        try {
            GenericRecordReader.readGenericRecordFromProtoByteString(avroLoad, outputSchema, dataConsumer);
        }catch(IOException ioe){
            throw new CacheNotFoundException(String.format("Failed to load avro record"));
        }
        return outputSchema;
    }
}
