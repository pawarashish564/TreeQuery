package org.treequery.service;

import com.google.common.collect.Lists;
import lombok.NonNull;
import org.treequery.beam.BeamPipelineBuilderImpl;
import org.treequery.beam.cache.BeamCacheOutputBuilder;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.cluster.Cluster;
import org.treequery.cluster.ClusterDependencyGraph;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.execute.GraphNodePipeline;
import org.treequery.execute.NodePipeline;
import org.treequery.execute.NodeTraverser;
import org.treequery.execute.PipelineBuilderInterface;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.AvroSchemaHelper;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;


import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

@Slf4j
@Builder
public class TreeQueryClusterRunnerImpl implements TreeQueryClusterRunner {
    CacheTypeEnum cacheTypeEnum;
    @NonNull
    BeamCacheOutputBuilder beamCacheOutputBuilder;
    @NonNull
    AvroSchemaHelper avroSchemaHelper;
    @NonNull
    DiscoveryServiceInterface discoveryServiceInterface;
    @NonNull
    Cluster atCluster;
    TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;

    //Output is found in AvroIOHelper.getPageRecordFromAvroCache
    @Override
    public void runQueryTreeNetwork(Node rootNode, Consumer<StatusTreeQueryCluster> statusCallback) {
        ClusterDependencyGraph clusterDependencyGraph = ClusterDependencyGraph.createClusterDependencyGraph(rootNode);
        Cluster rootCluster = rootNode.getCluster();
        Optional.ofNullable(this.treeQueryClusterRunnerProxyInterface).orElseThrow(()->{
            statusCallback.accept(
                    StatusTreeQueryCluster.builder()
                            .status(StatusTreeQueryCluster.QueryTypeEnum.SYSTEMERROR)
                            .description("Not found treeQueryClusterProxyInteface in remote call")
                            .node(rootNode)
                            .cluster(atCluster)
                            .build());
            return new IllegalStateException("Not found treeQueryClusterProxyInteface in remote call");
        });

        while (true){
            List<Node> nodeList = clusterDependencyGraph.popClusterWithoutDependency();
            if (nodeList.size()==0){
                break;
            }
            for (Node node: nodeList) {

                if (atCluster.equals(node.getCluster())){
                    log.debug(String.format("Local Run: Cluster %s %s", node.toString(), node.getName()));
                    try {
                        this.executeBeamRun(node, beamCacheOutputBuilder.createBeamCacheOutputImpl(), statusCallback);
                    }catch(IllegalStateException ie){
                        log.error(ie.getMessage());
                    }
                    catch(Throwable ex){
                        log.error(ex.getMessage());
                        statusCallback.accept(
                                StatusTreeQueryCluster.builder()
                                        .status(StatusTreeQueryCluster.QueryTypeEnum.SYSTEMERROR)
                                        .description(ex.getMessage())
                                        .node(node)
                                        .cluster(atCluster)
                                        .build()
                        );
                    }
                }else{
                    log.debug(String.format("RPC call: Cluster %s %s", node.toString(), node.getName()));
                    //It should be RPC call...
                    //the execution behavior depends on the injected TreeQueryClusterRunnerProxyInterface
                    Optional.ofNullable(this.treeQueryClusterRunnerProxyInterface)
                            .orElseThrow(()->{
                                statusCallback.accept(
                                        StatusTreeQueryCluster.builder()
                                                .status(StatusTreeQueryCluster.QueryTypeEnum.SYSTEMERROR)
                                                .description("Not found treeQueryClusterProxyInteface in remote call")
                                                .node(node)
                                                .cluster(atCluster)
                                                .build()
                                );
                                return new IllegalStateException("Not found treeQueryClusterProxyInteface in remote call");
                            })
                            .runQueryTreeNetwork(node, statusCallback);

                }
            }
        }

        statusCallback.accept(StatusTreeQueryCluster.builder()
                                .status(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS)
                                .description("OK:"+rootNode.getName())
                                .node(rootNode)
                                .cluster(rootNode.getCluster())
                                .build());
    }

    @Override
    public void setTreeQueryClusterRunnerProxyInterface(TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface) {
        this.treeQueryClusterRunnerProxyInterface = treeQueryClusterRunnerProxyInterface;
    }

    private void executeBeamRun(Node node, BeamCacheOutputInterface beamCacheOutputInterface, Consumer<StatusTreeQueryCluster> statusCallback){
        //Apache Beam pipeline runner creation
        PipelineBuilderInterface pipelineBuilderInterface =  BeamPipelineBuilderImpl.builder()
                .beamCacheOutputInterface(beamCacheOutputInterface)
                .avroSchemaHelper(avroSchemaHelper)
                .discoveryServiceInterface(discoveryServiceInterface)
                .build();

        //Inject Apache Beam pipeline runner
        NodePipeline nodePipeline = GraphNodePipeline.builder()
                .cluster(node.getCluster())
                .pipelineBuilderInterface(pipelineBuilderInterface)
                .cacheTypeEnum(cacheTypeEnum)
                .avroSchemaHelper(avroSchemaHelper)
                .build();
        List<Node> traversedResult = Lists.newLinkedList();
        NodeTraverser.postOrderTraversalExecution(node, null, traversedResult,nodePipeline );
        nodePipeline.getPipelineBuilder();

        //Execeute the Pipeline runner
        try {
            pipelineBuilderInterface.executePipeline();
        }catch(Throwable ex){
            log.error(ex.getMessage());

            statusCallback.accept(
                    StatusTreeQueryCluster.builder()
                            .status(StatusTreeQueryCluster.QueryTypeEnum.FAIL)
                            .description(ex.getMessage())
                            .cluster(node.getCluster())
                            .build()
            );
            throw new IllegalStateException("Failure run : "+ex.getMessage());
        }
    }
}
