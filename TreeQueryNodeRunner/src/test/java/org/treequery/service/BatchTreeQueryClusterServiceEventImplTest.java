package org.treequery.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.Mock;

import org.mockito.junit.jupiter.MockitoExtension;
import org.treequery.Transform.JoinNode;
import org.treequery.config.TreeQuerySetting;
import org.treequery.dto.TreeNodeEvent;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.EventBus.BasicTreeNodeEventBusTrafficLightImpl;
import org.treequery.utils.EventBus.TreeNodeEventBusTrafficLight;
import org.treequery.utils.JsonInstructionHelper;
import org.treequery.utils.TestDataAgent;
import org.treequery.utils.TreeQuerySettingHelper;

import org.treequery.model.Node;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@Slf4j
@ExtendWith(MockitoExtension.class)
class BatchTreeQueryClusterServiceEventImplTest {
    private static TreeQuerySetting treeQuerySetting;
    private static int WAITSECOND = 60 ;
    Node node;
    TreeNodeEventBusTrafficLight<Node> eventTreeNodeEventBusTrafficLight;
    TreeQueryClusterRunner localTreeQueryClusterRunner;
    TreeQueryClusterRunner remoteTreeQueryClusterRunner;



    @BeforeAll
    static void initAll(){
        treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
    }

    @BeforeEach
    void initTreeNode() throws Exception{
        String AvroTree = "TreeQueryInput4.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        node = JsonInstructionHelper.createNode(jsonString);

        eventTreeNodeEventBusTrafficLight = new BasicTreeNodeEventBusTrafficLightImpl();
        localTreeQueryClusterRunner = new LocalTreeQueryClusterRunner(
                treeQuerySetting, StatusTreeQueryCluster.QueryTypeEnum.SUCCESS
        );
        remoteTreeQueryClusterRunner = new RemoteTreeQueryClusterRunner(
            treeQuerySetting, StatusTreeQueryCluster.QueryTypeEnum.SUCCESS
        );
    }

    @Test
    void runQueryTreeNetwork() throws Exception{

        TreeQueryClusterService batchTreeQueryClusterServiceEvent = BatchTreeQueryClusterServiceEventImpl.builder()
                .localTreeQueryClusterRunner(localTreeQueryClusterRunner)
                .remoteTreeQueryClusterRunner(remoteTreeQueryClusterRunner)
                .treeQuerySetting(treeQuerySetting)
                .eventTreeNodeEventBusTrafficLight(eventTreeNodeEventBusTrafficLight)
                .build();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicInteger okCounter = new AtomicInteger();
        batchTreeQueryClusterServiceEvent.runQueryTreeNetwork(
                node,
                statusTreeQueryCluster -> {

                    try {
                        assertThat(statusTreeQueryCluster.getStatus()).isEqualByComparingTo(StatusTreeQueryCluster.QueryTypeEnum.SUCCESS);
                        okCounter.incrementAndGet();
                    }finally {
                        countDownLatch.countDown();
                    }
                }
        );

        countDownLatch.await(WAITSECOND, TimeUnit.SECONDS);
        assertThat(okCounter.get()).isGreaterThan(0);

    }

    @Test
    void shouldThrowException() throws Exception{

        localTreeQueryClusterRunner = new LocalTreeQueryClusterRunner(
                treeQuerySetting, StatusTreeQueryCluster.QueryTypeEnum.FAIL
        );
        remoteTreeQueryClusterRunner = new RemoteTreeQueryClusterRunner(
                treeQuerySetting, StatusTreeQueryCluster.QueryTypeEnum.FAIL
        );
        TreeQueryClusterService batchTreeQueryClusterServiceEvent = BatchTreeQueryClusterServiceEventImpl.builder()
                .localTreeQueryClusterRunner(localTreeQueryClusterRunner)
                .remoteTreeQueryClusterRunner(remoteTreeQueryClusterRunner)
                .treeQuerySetting(treeQuerySetting)
                .eventTreeNodeEventBusTrafficLight(eventTreeNodeEventBusTrafficLight)
                .build();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        batchTreeQueryClusterServiceEvent.runQueryTreeNetwork(
                node,
                statusTreeQueryCluster -> {
                    log.debug("Get failure message:"+statusTreeQueryCluster.toString());
                    assertThat(statusTreeQueryCluster.getStatus()).isEqualByComparingTo(StatusTreeQueryCluster.QueryTypeEnum.FAIL);
                    countDownLatch.countDown();
                }
        );
        countDownLatch.await(WAITSECOND, TimeUnit.SECONDS);

    }

    @Slf4j
    @RequiredArgsConstructor
    static class LocalTreeQueryClusterRunner implements TreeQueryClusterRunner{
        final TreeQuerySetting treeQuerySetting;
        final StatusTreeQueryCluster.QueryTypeEnum queryTypeEnum;
        @Override
        public void runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> StatusCallback) {
            log.debug("Running Local node:"+node.toJson());
            StatusTreeQueryCluster statusTreeQueryCluster = StatusTreeQueryCluster.builder()
                    .cluster(treeQuerySetting.getCluster())
                    .description(node.getDescription())
                    .node(node)
                    .status(queryTypeEnum)
                    .build();
            StatusCallback.accept(statusTreeQueryCluster);
        }

        @Override
        public String toString() {
            return "LocalCluster";
        }

        @Override
        public void setTreeQueryClusterRunnerProxyInterface(TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface) {
            throw new NoSuchMethodError();
        }
    }

    @Slf4j
    @RequiredArgsConstructor
    static class RemoteTreeQueryClusterRunner implements TreeQueryClusterRunner{
        final TreeQuerySetting treeQuerySetting;
        final StatusTreeQueryCluster.QueryTypeEnum queryTypeEnum;
        @Override
        public void runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> StatusCallback) {
            log.debug("Running Remote node:"+node.toJson());
            StatusTreeQueryCluster statusTreeQueryCluster = StatusTreeQueryCluster.builder()
                    .cluster(treeQuerySetting.getCluster())
                    .description(node.getDescription())
                    .node(node)
                    .status(queryTypeEnum)
                    .build();
            StatusCallback.accept(statusTreeQueryCluster);
        }

        @Override
        public String toString() {
            return "RemoteCluster";
        }

        @Override
        public void setTreeQueryClusterRunnerProxyInterface(TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface) {
            throw new NoSuchMethodError();
        }
    }

}