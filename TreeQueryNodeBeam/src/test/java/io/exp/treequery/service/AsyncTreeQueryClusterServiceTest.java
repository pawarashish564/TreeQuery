package io.exp.treequery.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.exp.treequery.Transform.TransformNodeFactory;
import io.exp.treequery.beam.cache.BeamCacheOutputInterface;
import io.exp.treequery.cluster.NodeFactory;
import io.exp.treequery.cluster.NodeTreeFactory;
import io.exp.treequery.execute.cache.CacheInputInterface;
import io.exp.treequery.execute.cache.FileCacheInputImpl;
import io.exp.treequery.model.Node;
import io.exp.treequery.util.JsonInstructionHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AsyncTreeQueryClusterServiceTest {

    TreeQueryClusterService treeQueryClusterService = null;
    TreeQueryClusterRunnerFactory treeQueryClusterRunnerFactory = null;
    BeamCacheOutputInterface beamCacheOutputInterface = null;
    CacheInputInterface cacheInputInterface = null;
    @BeforeEach
    void init(){
        treeQueryClusterRunnerFactory = mock(TreeQueryClusterRunnerFactory.class);
        beamCacheOutputInterface = mock(BeamCacheOutputInterface.class);
        cacheInputInterface = mock(CacheInputInterface.class);
        when(treeQueryClusterRunnerFactory.createTreeQueryClusterRunner()).then(
                invocation -> {
                    return SimpleLocalTreeQueryClusterRunnerImpl.builder()
                            .beamCacheOutputInterface(beamCacheOutputInterface)
                            .cacheInputInterface(cacheInputInterface)
                            .build();
                }
        );
         treeQueryClusterService =  AsyncTreeQueryClusterService.builder()
                 .treeQueryClusterRunnerFactory(()->{
                     return SimpleLocalTreeQueryClusterRunnerImpl.builder()
                             .beamCacheOutputInterface(beamCacheOutputInterface)
                             .cacheInputInterface(cacheInputInterface)
                             .build();
                 })
                 .build();
    }

    private String prepareNodeFromJsonInstruction(String jsonFileName){
        String workDirectory = null;
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File jsonFile = new File(classLoader.getResource(jsonFileName).getFile());
        workDirectory = jsonFile.getParent();

        String jsonString = JsonInstructionHelper.parseJsonFile(jsonFile.getAbsolutePath());
        return jsonString.replaceAll("\\$\\{WORKDIR\\}", workDirectory);
    }

    private Node createNode(String jsonString) throws JsonProcessingException {
        Node rootNode = null;
        NodeFactory nodeFactory;
        NodeTreeFactory nodeTreeFactory;
        nodeFactory = new TransformNodeFactory();
        nodeTreeFactory = NodeTreeFactory.builder().nodeFactory(nodeFactory).build();
        rootNode = nodeTreeFactory.parseJsonString(jsonString);
        return rootNode;
    }

    @Test
    void runAsyncService() throws Exception{
        String AvroTree = "SimpleAvroReadCluster.json";
        String jsonString = prepareNodeFromJsonInstruction(AvroTree);
        Node rootNode = createNode(jsonString);


        treeQueryClusterService.runQueryTreeNetwork(rootNode, (status)->{
            synchronized (rootNode) {
                rootNode.notify();
            }
        });
        synchronized (rootNode){
            rootNode.wait();
        }
    }
}