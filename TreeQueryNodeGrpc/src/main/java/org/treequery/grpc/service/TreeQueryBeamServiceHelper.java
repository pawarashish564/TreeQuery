package org.treequery.grpc.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.beam.cache.FileBeamCacheOutputImpl;
import org.treequery.beam.cache.RedisCacheOutputImpl;
import org.treequery.model.AvroSchemaHelper;
import org.treequery.model.BasicAvroSchemaHelper;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.AsyncTreeQueryClusterService;
import org.treequery.service.TreeQueryClusterRunnerImpl;
import org.treequery.service.TreeQueryClusterService;
import org.treequery.utils.JsonInstructionHelper;

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

    public void process(TreeQueryRequest treeQueryRequest) {
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

        }

    }

    private GenericRecord runQuery(Node rootNode, long pageSize, long page){

        treeQueryClusterService.runQueryTreeNetwork(rootNode, (status)->{
            log.debug(status.toString());
            synchronized (rootNode) {
                rootNode.notify();
            }
        });

        GenericRecord genericRecord = null;

        return genericRecord;
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
