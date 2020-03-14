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
import io.exp.treequery.model.Node;
import lombok.Builder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;
import java.util.function.Consumer;


@Builder
public class SimpleLocalTreeQueryClusterRunnerImpl implements TreeQueryClusterRunner {
    CacheInputInterface cacheInputInterface;
    BeamCacheOutputInterface beamCacheOutputInterface;

    @Override
    public void runQueryTreeNetwork(Node rootNode, Consumer<StatusTreeQueryCluster> statusCallback) {
        ClusterDependencyGraph clusterDependencyGraph = ClusterDependencyGraph.createClusterDependencyGraph(rootNode);
        String hashCode = rootNode.getIdentifier();
        while (true){
            List<Node> nodeList = clusterDependencyGraph.findClusterWithoutDependency();
            if (nodeList.size()==0){
                break;
            }
            for (Node node: nodeList) {
                PipelineBuilderInterface pipelineBuilderInterface =  BeamPipelineBuilderImpl.builder()
                        .beamCacheOutputInterface(beamCacheOutputInterface)
                        .build();

                NodePipeline nodePipeline = GraphNodePipeline.builder()
                        .cluster(node.getCluster())
                        .pipelineBuilderInterface(pipelineBuilderInterface)
                        .cacheInputInterface(cacheInputInterface)
                        .build();
                List<Node> traversedResult = Lists.newLinkedList();
                NodeTraverser.postOrderTraversalExecution(node, null, traversedResult,nodePipeline );
                nodePipeline.getPipelineBuilder();
                Pipeline pipeline = pipelineBuilderInterface.getPipeline();

                //Final result Pcollection
                PCollection<GenericRecord> record = pipelineBuilderInterface.getPCollection(node);

                this.beamCacheOutputInterface.writeGenericRecord(record, String.format("%s.avro", hashCode));
                //Most simple runner
                pipeline.run();
                clusterDependencyGraph.removeClusterDependency(node);
            }
        }

        statusCallback.accept(new StatusTreeQueryCluster());
    }
}
