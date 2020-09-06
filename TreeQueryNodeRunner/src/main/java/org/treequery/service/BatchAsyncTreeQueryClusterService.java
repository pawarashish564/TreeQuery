package org.treequery.service;

import com.google.common.collect.Maps;
import org.treequery.model.Node;
import lombok.Builder;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;


public class BatchAsyncTreeQueryClusterService implements TreeQueryClusterService {
    private static final int MAX_THREAD_RUN = 10;
    ExecutorService executor = Executors.newFixedThreadPool(MAX_THREAD_RUN);
    Map<String, TreeQueryClusterRunner> treeQueryClusterRunnerMap = Maps.newConcurrentMap();

    TreeQueryClusterRunnerFactory treeQueryClusterRunnerFactory;
    @Builder
    BatchAsyncTreeQueryClusterService(TreeQueryClusterRunnerFactory treeQueryClusterRunnerFactory){
        this.treeQueryClusterRunnerFactory = treeQueryClusterRunnerFactory;
    }

    @Override
    public String runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> statusCallback) {
        String uniqueId = UUID.randomUUID().toString();
        TreeQueryClusterRunner treeQueryClusterRunner = treeQueryClusterRunnerFactory.createTreeQueryClusterRunner();
        treeQueryClusterRunnerMap.put(uniqueId, treeQueryClusterRunner);

        Runnable runnableTask = () -> {
            treeQueryClusterRunner.runQueryTreeNetwork(node, statusCallback);
        };
        executor.execute(runnableTask);

        return uniqueId;
    }
}
