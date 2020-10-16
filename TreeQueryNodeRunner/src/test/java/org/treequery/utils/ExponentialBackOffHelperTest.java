package org.treequery.utils;

import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.treequery.cluster.Cluster;
import org.treequery.exception.FailClusterRunException;
import org.treequery.model.Node;
import org.treequery.service.RemoteProxyTreeQueryClusterRunner;
import org.treequery.service.StatusTreeQueryCluster;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ExponentialBackOffHelperTest {
    static int MAX_RETRIES = 3;
    static int INITIAL_INTERVAL = 2*1000;
    static double MULTIPLIER = 1.1;

    Node node;

    @RequiredArgsConstructor
    static class RemoteInput{
        final Node node;
        final Consumer<StatusTreeQueryCluster> statusCallback;
    }

    @Mock
    RemoteProxyTreeQueryClusterRunner remoteProxyTreeQueryClusterRunner;

    @BeforeEach
    void initFuncObject() throws Exception{
        String AvroTree = "TreeQueryInput4.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        node = JsonInstructionHelper.createNode(jsonString);
    }



    @Test
    public void shouldExceedMaxTry() throws Exception {
        int trial = 0;
        doThrow(
                new FailClusterRunException(
                        StatusTreeQueryCluster.builder()
                                .description("Test")
                                .node(node)
                                .cluster(Cluster.builder().build())
                                .status(StatusTreeQueryCluster.QueryTypeEnum.SYSTEMERROR).build()
                )
        ).when(remoteProxyTreeQueryClusterRunner).runQueryTreeNetwork(any(Node.class), any(Consumer.class));

        ExponentialBackOffHelper<Node, StatusTreeQueryCluster> exponentialBackOffHelper =
                ExponentialBackOffHelper.<Node, StatusTreeQueryCluster>builder()
                        .INITIAL_INTERVAL(INITIAL_INTERVAL)
                        .MAX_RETRIES(MAX_RETRIES)
                        .MULTIPLIER(MULTIPLIER).build();
        assertThrows(FailClusterRunException.class,
                ()->{
                    exponentialBackOffHelper.backOffCall("remotecall",
                            (node)->{
                                remoteProxyTreeQueryClusterRunner.runQueryTreeNetwork(node,
                                        statusTreeQueryCluster -> {});
                                return StatusTreeQueryCluster.builder()
                                        .description("Test")
                                        .node(node)
                                        .cluster(Cluster.builder().build())
                                        .status(StatusTreeQueryCluster.QueryTypeEnum.SYSTEMERROR).build();
                            }
                            , node);
                });

    }

}