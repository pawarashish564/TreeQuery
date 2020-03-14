package io.exp.treequery.service;

import com.google.common.collect.Lists;
import io.exp.treequery.beam.BeamPipelineBuilderImpl;
import io.exp.treequery.beam.cache.BeamCacheOutputInterface;
import io.exp.treequery.cluster.ClusterDependencyGraph;
import io.exp.treequery.execute.GraphNodePipeline;
import io.exp.treequery.execute.NodePipeline;
import io.exp.treequery.execute.NodeTraverser;
import io.exp.treequery.execute.PipelineBuilderInterface;
import io.exp.treequery.execute.cache.CacheInputInterface;
import io.exp.treequery.model.AvroSchemaHelper;
import io.exp.treequery.model.Node;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;
import java.util.function.Consumer;

@Slf4j
@Builder
public class TreeQueryClusterRunnerImpl implements TreeQueryClusterRunner {
    CacheInputInterface cacheInputInterface;
    BeamCacheOutputInterface beamCacheOutputInterface;
    AvroSchemaHelper avroSchemaHelper;

    @Override
    public void runQueryTreeNetwork(Node rootNode, Consumer<StatusTreeQueryCluster> statusCallback) {
        ClusterDependencyGraph clusterDependencyGraph = ClusterDependencyGraph.createClusterDependencyGraph(rootNode);

        while (true){
            List<Node> nodeList = clusterDependencyGraph.findClusterWithoutDependency();
            if (nodeList.size()==0){
                break;
            }
            for (Node node: nodeList) {

                //Apache Beam pipeline runner creation
                PipelineBuilderInterface pipelineBuilderInterface =  BeamPipelineBuilderImpl.builder()
                        .beamCacheOutputInterface(beamCacheOutputInterface)
                        .avroSchemaHelper(avroSchemaHelper)
                        .build();

                //Inject Apache Beam pipeline runner
                NodePipeline nodePipeline = GraphNodePipeline.builder()
                        .cluster(node.getCluster())
                        .pipelineBuilderInterface(pipelineBuilderInterface)
                        .cacheInputInterface(cacheInputInterface)
                        .build();
                List<Node> traversedResult = Lists.newLinkedList();
                NodeTraverser.postOrderTraversalExecution(node, null, traversedResult,nodePipeline );
                nodePipeline.getPipelineBuilder();

                //Execeute the Pipeline runner
                try {
                    pipelineBuilderInterface.executePipeline();
                }catch(Exception ex){
                    log.error(ex.getMessage());
                    statusCallback.accept(
                            StatusTreeQueryCluster.builder()
                                    .status(StatusTreeQueryCluster.QueryTypeEnum.FAIL)
                                    .description(ex.getMessage())
                                    .build()
                    );
                    return;
                }
                clusterDependencyGraph.removeClusterDependency(node);
            }
        }

        statusCallback.accept(StatusTreeQueryCluster.builder()
                                .status(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS)
                                .description("OK")
                                .build());
    }
}
