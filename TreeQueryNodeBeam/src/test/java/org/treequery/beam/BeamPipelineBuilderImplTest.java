package org.treequery.beam;

import com.google.common.collect.Lists;
import org.treequery.Transform.TransformNodeFactory;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.cluster.ClusterDependencyGraph;
import org.treequery.cluster.NodeFactory;
import org.treequery.cluster.NodeTreeFactory;
import org.treequery.execute.GraphNodePipeline;
import org.treequery.execute.NodeTraverser;
import org.treequery.model.AvroSchemaHelper;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.util.JsonInstructionHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.treequery.execute.NodePipeline;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Slf4j
class BeamPipelineBuilderImplTest {
    CacheTypeEnum cacheTypeEnum;
    BeamCacheOutputInterface beamCacheOutputInterface;
    String fileName = "bondtrade1.avro";

    String workDirectory = null;
    NodeFactory nodeFactory;
    NodeTreeFactory nodeTreeFactory;
    AvroSchemaHelper avroSchemaHelper;

    @BeforeEach
    void init(){
        cacheTypeEnum = CacheTypeEnum.FILE;
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File jsonFile = new File(classLoader.getResource(fileName).getFile());
        workDirectory = jsonFile.getParent();

        beamCacheOutputInterface = mock(BeamCacheOutputInterface.class);
        avroSchemaHelper = mock(AvroSchemaHelper.class);
    }

    @Test
    void simpleAvroReadPipeline() throws Exception {
        String simpleAvroTree = "SimpleAvroReadCluster.json";
        Node rootNode = null;

        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File jsonFile = new File(classLoader.getResource(simpleAvroTree).getFile());

        String jsonString = JsonInstructionHelper.parseJsonFile(jsonFile.getAbsolutePath());
        jsonString = jsonString.replaceAll("\\$\\{WORKDIR\\}", workDirectory);

        nodeFactory = new TransformNodeFactory();
        nodeTreeFactory = NodeTreeFactory.builder().nodeFactory(nodeFactory).build();
        rootNode = nodeTreeFactory.parseJsonString(jsonString);
        ClusterDependencyGraph clusterDependencyGraph = ClusterDependencyGraph.createClusterDependencyGraph(rootNode);

        List<Node> nodeList = null;
        nodeList = clusterDependencyGraph.findClusterWithoutDependency();
        assertThat(nodeList).hasSize(1);

        for (Node node: nodeList){
            BeamPipelineBuilderImpl pipelineBuilderInterface =  BeamPipelineBuilderImpl.builder()
                                            .beamCacheOutputInterface(beamCacheOutputInterface)
                                            .build();

            NodePipeline nodePipeline = GraphNodePipeline.builder()
                    .cluster(node.getCluster())
                    .pipelineBuilderInterface(pipelineBuilderInterface)
                    .cacheTypeEnum(cacheTypeEnum)
                    .avroSchemaHelper(avroSchemaHelper)
                    .build();
            List<Node> traversedResult = Lists.newLinkedList();
            NodeTraverser.postOrderTraversalExecution(node, null, traversedResult,nodePipeline );
            nodePipeline.getPipelineBuilder();

            Pipeline pipeline = pipelineBuilderInterface.getPipeline();

            PCollection<GenericRecord> record = pipelineBuilderInterface.getPCollection(node);
            PAssert.that(record).satisfies((input)->{
                AtomicInteger cnt = new AtomicInteger();
                input.forEach(
                        avroR->{
                            GenericRecord avroRecord = (GenericRecord) avroR;
                            assertThat(((Utf8)avroRecord.get("id")).toString()).isNotBlank();
                            GenericData.Record assetRecord = (GenericData.Record)avroRecord.get("asset");
                            assertThat(((Utf8)assetRecord.get("securityId")).toString()).isNotBlank();
                            assertThat(((Double)assetRecord.get("notional"))).isNotNaN();
                            cnt.incrementAndGet();
                        }
                );
                assertThat(cnt.get()).isEqualTo(1000);
                return null;
            });
            pipeline.run();
            clusterDependencyGraph.removeClusterDependency(node);
        }

    }






}