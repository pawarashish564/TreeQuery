package org.treequery.beam.transform;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.model.CacheNode;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.utils.proxy.TreeQueryClusterAvroCacheInterface;
import org.treequery.utils.proxy.TreeQueryClusterAvroCacheProxyFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class CacheBeamHelper implements NodeBeamHelper {
    private final DiscoveryServiceInterface discoveryServiceInterface;
    private final TreeQueryClusterAvroCacheInterface treeQueryClusterAvroCacheInterface;
    private final TreeQuerySetting treeQuerySetting;
    @Builder
    CacheBeamHelper(TreeQuerySetting treeQuerySetting, DiscoveryServiceInterface discoveryServiceInterface){
        this.discoveryServiceInterface = discoveryServiceInterface;
        this.treeQuerySetting = treeQuerySetting;
        treeQueryClusterAvroCacheInterface = TreeQueryClusterAvroCacheProxyFactory
                                                .getDefaultCacheInterface(treeQuerySetting, this.discoveryServiceInterface);
    }

    @Override
    public PCollection<GenericRecord> apply(Pipeline pipeline, List<PCollection<GenericRecord>> parentCollectionLst, Node node) {
        if (!( node instanceof CacheNode)){
            throw new IllegalArgumentException(String.format("%s is not Cache Node", node.toString()));
        }
        String identifier = node.getIdentifier();
        Schema schema ;
        try {
            log.debug(String.format("Get cache Node: %s with identifier %s : %s", node.getName(), node.getIdentifier(),node.toJson()));
            //Get the Schema first
            schema = treeQueryClusterAvroCacheInterface
                    .getPageRecordFromAvroCache(null, CacheTypeEnum.FILE, identifier, 1, 1, (data) -> {
                    });
        }catch(CacheNotFoundException che){
            log.error(che.getMessage());
            throw new IllegalStateException(String.format("Failed to retrieve cache for %s(%s)", node.getName() ,identifier));
        }
        PCollection<String> identifierCollection = pipeline.apply(Create.of(identifier));
        PCollection<GenericRecord> genericRecordPCollection = identifierCollection.apply(
            new CacheReadTransform(treeQueryClusterAvroCacheInterface, schema)
        );
        return genericRecordPCollection;
    }

    @RequiredArgsConstructor
    private static class CacheReadTransform extends PTransform< PCollection<String>, PCollection<GenericRecord> > {
        private final TreeQueryClusterAvroCacheInterface treeQueryClusterAvroCacheInterface;
        private final Schema schema;
        @Override
        public PCollection<GenericRecord> expand(PCollection<String> input) {
            AvroCoder coder = AvroCoder.of(GenericRecord.class, schema);
            PCollection<GenericRecord> recordPCollection = input.apply(
                    ParDo.of(new ReadFunction(treeQueryClusterAvroCacheInterface))
            ).setCoder(coder);
            return recordPCollection;
        }
        @RequiredArgsConstructor
        private static class ReadFunction extends DoFn<String, GenericRecord> {
            private final TreeQueryClusterAvroCacheInterface treeQueryClusterAvroCacheInterface;
            @ProcessElement
            public void processElement(@Element String identifier, OutputReceiver<GenericRecord > out) throws CacheNotFoundException {
                AtomicLong counter = new AtomicLong(0);

                int page = 1;
                int pageSize = 100;

                while(true){
                    long lastCount = counter.get();
                    treeQueryClusterAvroCacheInterface.getPageRecordFromAvroCache(null,
                            CacheTypeEnum.FILE, identifier, pageSize, page, (record) -> {
                        counter.incrementAndGet();
                        out.output(record);
                    });
                    if (counter.get() == lastCount){
                        break;
                    }
                }
            }
        }
    }
}
