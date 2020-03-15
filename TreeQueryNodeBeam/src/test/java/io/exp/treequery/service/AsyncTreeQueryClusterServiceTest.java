package io.exp.treequery.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.exp.treequery.Transform.LoadLeafNode;
import io.exp.treequery.Transform.TransformNodeFactory;
import io.exp.treequery.beam.cache.BeamCacheOutputInterface;
import io.exp.treequery.cluster.NodeFactory;
import io.exp.treequery.cluster.NodeTreeFactory;
import io.exp.treequery.model.AvroSchemaHelper;
import io.exp.treequery.model.CacheTypeEnum;
import io.exp.treequery.model.DataSource;
import io.exp.treequery.model.Node;
import io.exp.treequery.util.JsonInstructionHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.matchers.Any;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
@Slf4j
class AsyncTreeQueryClusterServiceTest {

    TreeQueryClusterService treeQueryClusterService = null;

    BeamCacheOutputInterface beamCacheOutputInterface = null;
    CacheTypeEnum cacheTypeEnum;
    AvroSchemaHelper avroSchemaHelper = null;
    @BeforeEach
    void init() throws IOException {

        cacheTypeEnum = CacheTypeEnum.FILE;
        avroSchemaHelper = mock(AvroSchemaHelper.class);

        Path path = Files.createTempDirectory("TreeQuery_");
        log.debug("Write temp result into "+path.toAbsolutePath().toString());


        beamCacheOutputInterface = new BeamCacheOutputInterface() {
            @Override
            public void writeGenericRecord(PCollection<GenericRecord> stream, Schema avroSchema, String outputLabel) {
                    String fileName = String.format("%s/%s", path.toAbsolutePath().toString(), outputLabel);
                    stream.apply(
                            AvroIO.writeGenericRecords(avroSchema).to(fileName).withoutSharding().withSuffix(".avro")
                    );
            }
        };


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
        LoadLeafNode d = (LoadLeafNode) rootNode;
        when(avroSchemaHelper.getAvroSchema(rootNode)).then(
                (node)-> {
                    return d.getAvroSchemaObj();
                }
        );

        treeQueryClusterService =  AsyncTreeQueryClusterService.builder()
                .treeQueryClusterRunnerFactory(()->{
                    return TreeQueryClusterRunnerImpl.builder()
                            .beamCacheOutputInterface(beamCacheOutputInterface)
                            .cacheTypeEnum(cacheTypeEnum)
                            .avroSchemaHelper(avroSchemaHelper)
                            .build();
                })
                .build();

        treeQueryClusterService.runQueryTreeNetwork(rootNode, (status)->{
            log.debug(status.toString());
            synchronized (rootNode) {
                rootNode.notify();
            }
            assertThat(status.status).isEqualTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
        });
        synchronized (rootNode){
            rootNode.wait();
        }
    }
}