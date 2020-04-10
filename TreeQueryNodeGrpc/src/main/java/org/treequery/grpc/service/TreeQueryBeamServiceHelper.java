package org.treequery.grpc.service;

import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.beam.cache.FileBeamCacheOutputImpl;
import org.treequery.beam.cache.RedisCacheOutputImpl;
import org.treequery.exception.TimeOutException;
import org.treequery.model.AvroSchemaHelper;
import org.treequery.model.BasicAvroSchemaHelper;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.AsyncTreeQueryClusterService;
import org.treequery.service.StatusTreeQueryCluster;
import org.treequery.service.TreeQueryClusterRunnerImpl;
import org.treequery.service.TreeQueryClusterService;
import org.treequery.utils.AsyncRunHelper;
import org.treequery.utils.JsonInstructionHelper;

import java.io.File;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

@Slf4j
public class TreeQueryBeamServiceHelper {
    TreeQueryClusterService treeQueryClusterService;
    BeamCacheOutputInterface beamCacheOutputInterface = null;
    CacheTypeEnum cacheTypeEnum;
    AvroSchemaHelper avroSchemaHelper = null;

    public TreeQueryBeamServiceHelper(CacheTypeEnum cacheTypeEnum){
        cacheTypeEnum = CacheTypeEnum.FILE;
        avroSchemaHelper = new BasicAvroSchemaHelper();
        beamCacheOutputInterface = getCacheOutputImpl(cacheTypeEnum);
        init();
    }

    public void init(){
        treeQueryClusterService =  AsyncTreeQueryClusterService.builder()
                .treeQueryClusterRunnerFactory(()->
                        TreeQueryClusterRunnerImpl.builder()
                            .beamCacheOutputInterface(beamCacheOutputInterface)
                            .cacheTypeEnum(cacheTypeEnum)
                            .avroSchemaHelper(avroSchemaHelper)
                            .build())
                .build();
    }

    public PreprocessInput preprocess(String jsonInput){
        Node rootNode;
        Schema outputSchema;
        try {
            rootNode = JsonInstructionHelper.createNode(jsonInput);
            outputSchema = avroSchemaHelper.getAvroSchema(rootNode);
        }catch(Exception je){
            throw new IllegalArgumentException(String.format("Not able to parse:%s", jsonInput));
        }
        return PreprocessInput.builder()
                .node(rootNode)
                .outputSchema(outputSchema)
                .build();
    }

    public ReturnResult process(TreeQueryRequest.RunMode runMode,
                                PreprocessInput preprocessInput,
                                       boolean renewCache,
                                       long pageSize,
                                       long page,
                                Consumer<GenericRecord> dataConsumer) {

        String identifier = preprocessInput.node.getIdentifier();

        if (!renewCache){
            throw new IllegalStateException("Not yet implemented cache");
        }
        return this.runQuery(preprocessInput.node, pageSize, page, dataConsumer);
    }

    private ReturnResult runQuery(Node rootNode, long pageSize, long page, Consumer<GenericRecord> dataConsumer){
        final AsyncRunHelper asyncRunHelper =  AsyncRunHelper.of(rootNode);
        final String hashCode = rootNode.getIdentifier();
        treeQueryClusterService.runQueryTreeNetwork(rootNode, (status)->{
            log.debug(status.toString());
            asyncRunHelper.continueRun(status);
        });
        try {
            StatusTreeQueryCluster statusTreeQueryCluster = asyncRunHelper.waitFor();
            if(statusTreeQueryCluster.getStatus() != StatusTreeQueryCluster.QueryTypeEnum.SUCCESS){
                return ReturnResult.builder()
                        .statusTreeQueryCluster(statusTreeQueryCluster)
                        .build();
            }else{
                Schema schema = beamCacheOutputInterface.getPageRecord(pageSize, page, dataConsumer);
                return ReturnResult.builder()
                        .hashCode(hashCode)
                        .statusTreeQueryCluster(statusTreeQueryCluster)
                        .dataSchema(schema)
                        .build();
            }
        }catch(TimeOutException te){
            log.error(te.getMessage());
            throw new IllegalStateException(String.format("Time out:%s", rootNode.toString()));
        }
    }

    @Builder
    @Getter
    public static class PreprocessInput{
        @NonNull
        private final Node node;
        @NonNull
        private final Schema outputSchema;
    }

    @Builder
    @Getter
    public static class ReturnResult{
        @NonNull
        String hashCode;
        @NonNull
        StatusTreeQueryCluster statusTreeQueryCluster;
        Schema dataSchema;
    }

    static BeamCacheOutputInterface getCacheOutputImpl(CacheTypeEnum cacheTypeEnum){
        BeamCacheOutputInterface beamCacheOutputInterface;
        switch(cacheTypeEnum){
            case FILE:
                beamCacheOutputInterface =  new FileBeamCacheOutputImpl();
                break;
            case REDIS:
                beamCacheOutputInterface = new RedisCacheOutputImpl();
                break;
            default:
                throw new NoSuchElementException();
        }
        return beamCacheOutputInterface;
    }

}
