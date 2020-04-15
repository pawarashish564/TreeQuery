package org.treequery.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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

        BeamProcessSynchronizer beamProcessSynchronizer = new BeamProcessSynchronizer();

        while (true){
            //Wait for BeamProcessQueueSynchronizer to be cleared
            BeamProcessSynchronizer.SyncStatus syncStatus;
            do {
                syncStatus = beamProcessSynchronizer.canSubmitJob();
            }while (syncStatus == BeamProcessSynchronizer.SyncStatus.WAIT);
            if (syncStatus != BeamProcessSynchronizer.SyncStatus.GO){
                StatusTreeQueryCluster.QueryTypeEnum queryTypeEnum = StatusTreeQueryCluster.QueryTypeEnum.FAIL;
                switch (syncStatus){
                    case SYSTEM_ERROR:
                        queryTypeEnum = StatusTreeQueryCluster.QueryTypeEnum.SYSTEMERROR;
                        break;
                    case FAIL:
                        queryTypeEnum = StatusTreeQueryCluster.QueryTypeEnum.FAIL;
                        break;
                }
                statusCallback.accept(StatusTreeQueryCluster.builder()
                        .status(queryTypeEnum)
                        .description("Exception:"+rootNode.getName())
                        .node(rootNode)
                        .cluster(rootNode.getCluster())
                        .build());
                break;
            }

            List<Node> nodeList = clusterDependencyGraph.popClusterWithoutDependency();

            if (nodeList.size()==0){
                break;
            }
            for (Node node: nodeList) {

                if (atCluster.equals(node.getCluster())){
                    log.debug(String.format("Local Run: Cluster %s %s", node.toString(), node.getName()));
                    try {
                        RunJob runJob = RunJob.createRunJob(node);
                        this.executeBeamRun(node, beamCacheOutputBuilder.createBeamCacheOutputImpl(),
                                (statusTreeQueryCluster)->{
                                    beamProcessSynchronizer.removeWaitItem(runJob.getUuid());
                                });
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
                        throw new IllegalStateException(ex.getMessage());
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



    @RequiredArgsConstructor
    @Getter
    static class RunJob{
        private final String uuid;
        private final Node node;
        static RunJob createRunJob(Node node){
            String uuid = UUID.randomUUID().toString();
            return new RunJob(uuid, node);
        }
    }

    private static class BeamProcessSynchronizer{
        private Map<String, RunJob> waitingJobMap = Maps.newConcurrentMap();
        List<St>
        enum SyncStatus{
            GO,WAIT,FAIL, SYSTEM_ERROR
        }
        boolean FAILURE = false;
        Object waitObj = new Object();

        SyncStatus canSubmitJob(){
            synchronized (waitingJobMap) {
                if (FAILURE){
                    return SyncStatus.FAIL;
                }
                if (waitingJobMap.size() == 0) {
                    return SyncStatus.GO;
                }else{
                    synchronized (waitObj){
                        try {
                            waitObj.wait();
                        }catch (InterruptedException ie){
                            log.error(ie.getMessage());
                        }
                    }
                    return SyncStatus.WAIT;
                }
            }
        }
        void pushWaitItem(Node node){
            RunJob runJob = RunJob.createRunJob(node);
            synchronized (waitingJobMap) {
                this.waitingJobMap.put(runJob.getUuid(), runJob);
            }
        }
        RunJob removeWaitItem(String uuid, StatusTreeQueryCluster statusTreeQueryCluster){
            synchronized (waitingJobMap) {
                RunJob runJob =  this.waitingJobMap.remove(uuid);
                synchronized (waitObj) {
                    waitObj.notifyAll();
                }
                return runJob;
            }
        }
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
