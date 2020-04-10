package org.treequery.grpc.service;

import lombok.extern.slf4j.Slf4j;
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

    public List<GenericRecord> process(TreeQueryRequest treeQueryRequest) {
        TreeQueryRequest.RunMode runMode = treeQueryRequest.getRunMode();
        String jsonInput = treeQueryRequest.getJsonInput();
        boolean renewCache = treeQueryRequest.getRenewCache();
        long pageSize = treeQueryRequest.getPageSize();
        long page = treeQueryRequest.getPage();

        Node rootNode;
        try {
            rootNode = JsonInstructionHelper.createNode(jsonInput);
        }catch(Exception je){
            throw new IllegalArgumentException(String.format("Not able to parse:%s", jsonInput));
        }
        String identifier = rootNode.getIdentifier();
        if (!renewCache){
            throw new IllegalStateException("Not yet implemented cache");
        }
        return this.runQuery(rootNode, pageSize, page);
    }

    private List<GenericRecord> runQuery(Node rootNode, long pageSize, long page){
        final AsyncRunHelper asyncRunHelper =  AsyncRunHelper.of(rootNode);
        treeQueryClusterService.runQueryTreeNetwork(rootNode, (status)->{
            log.debug(status.toString());
            asyncRunHelper.continueRun(status);
        });
        try {
            StatusTreeQueryCluster statusTreeQueryCluster = asyncRunHelper.waitFor();

        }catch(TimeOutException te){
            log.error(te.getMessage());
            throw new IllegalStateException(String.format("Time out:%s", rootNode.toString()));
        }
        List<GenericRecord> genericRecords = null;
        // beamCacheOutputInterface.getFile();
        return genericRecords;
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
