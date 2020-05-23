package org.treequery.beam.cache;

import com.google.common.base.Verify;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.ByteString;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericRecord;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.discoveryservicestatic.model.Location;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.grpc.client.GrpcClientChannel;
import org.treequery.grpc.exception.GrpcServiceException;
import org.treequery.grpc.exception.SchemaGetException;
import org.treequery.grpc.utils.GenericRecordReader;
import org.treequery.model.CacheTypeEnum;
import org.treequery.proto.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
public class StreamCacheProxy implements CacheInputInterface {
    private volatile Map<Cluster,
            Channel> ChannelMap = Maps.newConcurrentMap();

    private final PageCacheProxy pageCacheProxy;

    @NonNull
    private final DiscoveryServiceInterface discoveryServiceInterface;
    @Builder
    public StreamCacheProxy(DiscoveryServiceInterface discoveryServiceInterface){
        this.discoveryServiceInterface = discoveryServiceInterface;
        pageCacheProxy = new PageCacheProxy(this.discoveryServiceInterface);
    }

    @SneakyThrows
    private static Schema getRemoteSchema(Channel channel, String identifier){
        Schema schema = null;
        TreeQueryCacheServiceGrpc.TreeQueryCacheServiceBlockingStub treeQueryCacheServiceBlockingStub
                =TreeQueryCacheServiceGrpc.newBlockingStub(channel);
        try{
            SchemaRequest schemaRequest = SchemaRequest.newBuilder()
                    .setIdentifier(identifier).build();
            SchemaResponse schemaResponse = treeQueryCacheServiceBlockingStub.getSchema(schemaRequest);
            schema = new Schema.Parser().parse(schemaResponse.getAvroSchema());
        }catch(SchemaParseException se){
            throw se;
        } catch(Exception ex){
            Status status = Status.fromThrowable(ex);
            throw new SchemaGetException(status.getDescription());
        }
        return schema;
    }

    @Override
    public void getStreamRecordFromAvroCache(@Nullable Cluster cluster,
                                             String identifier,
                                             Consumer<GenericRecord> dataConsumer,
                                             @Nullable Schema schema) throws CacheNotFoundException {
        log.debug(String.format("Run Grpc Stream Avro Cache"));
        Cluster _cluster = Optional.ofNullable(cluster).orElse(discoveryServiceInterface.getCacheResultCluster(identifier));
        Optional.ofNullable(_cluster).orElseThrow(()->new CacheNotFoundException("No cluster for "+identifier));

        Channel channel = this.getChannel(_cluster);
        Schema avroSchema = Optional.ofNullable(schema).orElseGet(
                ()->getRemoteSchema(channel, identifier)
        );
        TreeQueryCacheServiceGrpc.TreeQueryCacheServiceStub treeQueryCacheServiceStub = TreeQueryCacheServiceGrpc.newStub(channel);
        CacheStreamRequest.Builder cacheStreamRequestBuilder = CacheStreamRequest.newBuilder();
        cacheStreamRequestBuilder.setIdentifier(identifier);
        CacheStreamObserver cacheStreamObserver = new CacheStreamObserver(dataConsumer, avroSchema);
        try{
            treeQueryCacheServiceStub.streamGet(cacheStreamRequestBuilder.build(),
                    cacheStreamObserver
            );
            cacheStreamObserver.waitUntilFinish();
        }catch(Exception ex){
            Status status = Status.fromThrowable(ex);
            throw new CacheNotFoundException(status.getDescription());
        }
    }

    @Override
    public Schema getPageRecordFromAvroCache(@Nullable Cluster cluster,
                                             String identifier,
                                             long pageSize, long page,
                                             Consumer<GenericRecord> dataConsumer,
                                             @Nullable Schema schema) throws CacheNotFoundException {

        return pageCacheProxy.getPageRecordFromAvroCache(cluster, identifier, pageSize, page,
                dataConsumer, schema);
    }
    @RequiredArgsConstructor
    private class CacheStreamObserver implements StreamObserver<CacheStreamResponse> {
        private final Consumer<GenericRecord> dataConsumer;
        private final Schema outputSchema;
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicInteger countError = new AtomicInteger(0);
        private final List<Throwable> errorStack = Lists.newLinkedList();

        @SneakyThrows
        @Override
        public void onNext(CacheStreamResponse value) {
            ByteString avroLoad = value.getAvroLoad();
            GenericRecordReader.readGenericRecordFromProtoByteString(avroLoad, outputSchema, dataConsumer);
        }

        @Override
        public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
            Status.Code code = status.getCode();
            Exception ex = new GrpcServiceException(status.getCode(), status.getDescription());
            errorStack.add(ex);
            countError.incrementAndGet();
            finishLatch.countDown();
        }

        @Override
        public void onCompleted() {
            log.debug("completed");
            finishLatch.countDown();
        }

        public Iterator<Throwable> getErrorIterator(){
            return this.errorStack.iterator();
        }
        public void waitUntilFinish(){
            if (!Uninterruptibles.awaitUninterruptibly(finishLatch, 1, TimeUnit.MINUTES)) {
                throw new RuntimeException("timeout!");
            }
            if (countError.get() > 0 ){
                throw new RuntimeException("Run with exceptions");
            }
        }
    }

    private Channel getChannel(Cluster cluster)
            throws CacheNotFoundException{
        ChannelMap.putIfAbsent(cluster, createNewStub(discoveryServiceInterface, cluster));
        return ChannelMap.get(cluster);
    }
    private static ManagedChannel createNewStub
            (DiscoveryServiceInterface discoveryServiceInterface, Cluster cluster){

        Location location = Optional.ofNullable(discoveryServiceInterface.getClusterLocation(cluster))
                .orElseThrow(()->new CacheNotFoundException("Cannot find location from " + cluster.toString()));
        GrpcClientChannel grpcClientChannel = new GrpcClientChannel(location.getAddress(), location.getPort());
        return (grpcClientChannel.getChannel());
    }
}
