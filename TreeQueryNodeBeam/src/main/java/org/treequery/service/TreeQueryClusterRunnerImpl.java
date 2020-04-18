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
import org.treequery.config.TreeQuerySetting;
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
import org.treequery.utils.proxy.TreeQueryClusterAvroCacheInterface;


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
    TreeQuerySetting treeQuerySetting;
    @NonNull
    TreeQueryClusterAvroCacheInterface treeQueryClusterAvroCacheInterface;

    TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;

    //Output is found in AvroIOHelper.getPageRecordFromAvroCache
    @Override
    public void runQueryTreeNetwork(Node rootNode, Consumer<StatusTreeQueryCluster> statusCallback) {
        Cluster atCluster = treeQuerySetting.getCluster();
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
                    final RunJob runJob = beamProcessSynchronizer.pushWaitItem(node);
                    try {
                        this.executeBeamRun(node, beamCacheOutputBuilder.createBeamCacheOutputImpl(),
                                (statusTreeQueryCluster)->{
                                    registerCacheResult(statusTreeQueryCluster);
                                    beamProcessSynchronizer.removeWaitItem(runJob.getUuid(), statusTreeQueryCluster);
                                });
                    } catch(Throwable ex){
                        log.error(ex.toString());
                        beamProcessSynchronizer.reportError(runJob.getUuid(), ex);
                        statusCallback.accept(
                                StatusTreeQueryCluster.builder()
                                        .status(StatusTreeQueryCluster.QueryTypeEnum.SYSTEMERROR)
                                        .description(ex.getMessage())
                                        .node(node)
                                        .cluster(atCluster)
                                        .build()
                        );
                        return;
                    }
                }else{
                    log.debug(String.format("RPC call: Cluster %s %s", node.toString(), node.getName()));
                    //It should be RPC call...
                    //the execution behavior depends on the injected TreeQueryClusterRunnerProxyInterface
                    final RunJob runJob = beamProcessSynchronizer.pushWaitItem(node);

                    if(this.treeQueryClusterRunnerProxyInterface!=null){
                        this.treeQueryClusterRunnerProxyInterface.runQueryTreeNetwork(node, (statusTreeQueryCluster)->{
                            //registerCacheResult(statusTreeQueryCluster);
                            beamProcessSynchronizer.removeWaitItem(runJob.getUuid(), statusTreeQueryCluster);
                        });
                    }else{
                        beamProcessSynchronizer.reportError(runJob.getUuid(), new IllegalStateException("Not found treeQueryClusterProxyInteface in remote call"));
                        statusCallback.accept(
                                StatusTreeQueryCluster.builder()
                                        .status(StatusTreeQueryCluster.QueryTypeEnum.SYSTEMERROR)
                                        .description("Not found treeQueryClusterProxyInteface in remote call")
                                        .node(node)
                                        .cluster(atCluster)
                                        .build()
                        );
                        return;
                    }
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


    private void registerCacheResult(StatusTreeQueryCluster statusTreeQueryCluster){
        boolean IsIssue = statusTreeQueryCluster.status!= StatusTreeQueryCluster.QueryTypeEnum.SUCCESS;
        Node node = statusTreeQueryCluster.getNode();
        String identifier = node.getIdentifier();

        if (!IsIssue){
            discoveryServiceInterface.registerCacheResult(identifier, node.getCluster());
            log.debug("Register "+node.getIdentifier()+" into "+node.getCluster());
            log.debug(String.format("Register Node: %s with identifier %s : %s", node.getName(), node.getIdentifier(),node.toJson()));
        }
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
        List<StatusTreeQueryCluster> statusLogList = Lists.newLinkedList();
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
        RunJob pushWaitItem(Node node){
            RunJob runJob = RunJob.createRunJob(node);
            synchronized (waitingJobMap) {
                this.waitingJobMap.put(runJob.getUuid(), runJob);
            }
            return runJob;
        }
        RunJob removeWaitItem(String uuid, StatusTreeQueryCluster statusTreeQueryCluster){
            synchronized (waitingJobMap) {
                RunJob runJob =  this.waitingJobMap.remove(uuid);
                if (statusTreeQueryCluster.status!= StatusTreeQueryCluster.QueryTypeEnum.SUCCESS){
                    this.FAILURE = true;
                }
                statusLogList.add(statusTreeQueryCluster);
                synchronized (waitObj) {
                    waitObj.notifyAll();
                }
                return runJob;
            }
        }
        RunJob reportError(String uuid, Throwable ex){
            synchronized (waitingJobMap) {
                this.FAILURE = true;
                RunJob runJob =  this.waitingJobMap.remove(uuid);
                statusLogList.add(
                        StatusTreeQueryCluster.builder()
                            .node(Optional.ofNullable(runJob).orElse(null).getNode())
                                .cluster(Optional.ofNullable(runJob).orElse(null).getNode().getCluster())
                                .description(ex.getMessage())
                                .status(StatusTreeQueryCluster.QueryTypeEnum.SYSTEMERROR)
                            .build()

                );
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
                .treeQueryClusterAvroCacheInterface(treeQueryClusterAvroCacheInterface)
                .treeQuerySetting(treeQuerySetting)
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
            log.debug("Run the pipeline");
            pipelineBuilderInterface.executePipeline();
            log.debug("Finished the pipeline");

            statusCallback.accept(
                    StatusTreeQueryCluster.builder()
                            .node(node)
                            .status(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS)
                            .description("Finished:"+node.getName())
                            .cluster(node.getCluster())
                            .build()
            );

        }catch(Throwable ex){
            log.error(ex.getMessage());

            statusCallback.accept(
                    StatusTreeQueryCluster.builder()
                            .node(node)
                            .status(StatusTreeQueryCluster.QueryTypeEnum.SYSTEMERROR)
                            .description(ex.getMessage())
                            .cluster(node.getCluster())
                            .build()
            );
        }
    }
}
