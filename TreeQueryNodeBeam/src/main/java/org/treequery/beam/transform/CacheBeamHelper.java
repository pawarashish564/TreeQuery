package org.treequery.beam.transform;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.model.CacheNode;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.beam.cache.CacheInputInterface;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class CacheBeamHelper implements NodeBeamHelper {
    private final DiscoveryServiceInterface discoveryServiceInterface;
    private final CacheInputInterface cacheInputInterface;
    private final TreeQuerySetting treeQuerySetting;

    @Builder
    CacheBeamHelper(TreeQuerySetting treeQuerySetting, DiscoveryServiceInterface discoveryServiceInterface, CacheInputInterface cacheInputInterface){
        this.discoveryServiceInterface = discoveryServiceInterface;
        this.treeQuerySetting = treeQuerySetting;
        this.cacheInputInterface = cacheInputInterface;
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
            schema = cacheInputInterface
                    .getPageRecordFromAvroCache(null, CacheTypeEnum.FILE, identifier, 1, 1, (data) -> {
                    }, ((CacheNode) node).getAvroSchemaObj());
        }catch(CacheNotFoundException che){
            log.error(che.getMessage());
            throw new IllegalStateException(String.format("Failed to retrieve cache for %s(%s)", node.getName() ,identifier));
        }
        PCollection<String> identifierCollection = pipeline.apply(Create.of(identifier));
        PCollection<GenericRecord> genericRecordPCollection = identifierCollection.apply(
            new CacheReadTransform(cacheInputInterface, schema)
        );
        return genericRecordPCollection;
    }

    @RequiredArgsConstructor
    private static class CacheReadTransform extends PTransform< PCollection<String>, PCollection<GenericRecord> > {
        private final CacheInputInterface cacheInputInterface;
        private final Schema schema;
        @Override
        public PCollection<GenericRecord> expand(PCollection<String> input) {
            AvroCoder coder = AvroCoder.of(GenericRecord.class, schema);
            Optional.ofNullable(cacheInputInterface).orElseThrow(()->new IllegalStateException("Cache interface is null"));
            PCollection<GenericRecord> recordPCollection = input.apply(
                    ParDo.of(new ReadFunction(cacheInputInterface))
            ).setCoder(coder);
            return recordPCollection;
        }
        @RequiredArgsConstructor
        private static class ReadFunction extends DoFn<String, GenericRecord> {
            private volatile static  CacheInputInterface cacheInputInterface;
            private Counter counter = Metrics.counter(ReadFunction.class, "ReadCacheCounter");

            ReadFunction(CacheInputInterface _CacheInputInterface){
                synchronized (ReadFunction.class) {
                    if (cacheInputInterface == null) {
                        cacheInputInterface = _CacheInputInterface;
                        counter.inc();
                    }
                }

            }

            @ProcessElement
            public void processElement(@Element String identifier, OutputReceiver<GenericRecord > out) throws CacheNotFoundException {
                AtomicLong counter = new AtomicLong(0);
                if (cacheInputInterface == null){
                    log.error("Failed to find CacheInputInterface instance for this run");
                    throw new IllegalStateException("Failed to find CacheInputInterface instance for this run");
                }
                int page = 1;
                int pageSize = 100;

                while(true){
                    long lastCount = counter.get();
                    Schema schema = null;
                    schema = cacheInputInterface.getPageRecordFromAvroCache(null,
                            CacheTypeEnum.FILE, identifier, pageSize, page, (record) -> {
                        counter.incrementAndGet();
                        out.output(record);
                    }, schema);
                    page++;
                    if (counter.get() == lastCount){
                        break;
                    }
                }
            }
        }
    }
}
