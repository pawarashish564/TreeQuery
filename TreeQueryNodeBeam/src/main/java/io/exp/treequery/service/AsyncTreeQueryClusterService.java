package io.exp.treequery.service;

import com.google.common.collect.Maps;
import io.exp.treequery.model.Node;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;


public class AsyncTreeQueryClusterService implements TreeQueryClusterService {
    private static final int MAX_THREAD_RUN = 10;
    ExecutorService executor = Executors.newFixedThreadPool(MAX_THREAD_RUN);
    Map<String, TreeQueryClusterRunner> treeQueryClusterRunnerMap = Maps.newConcurrentMap();

    @Override
    public String runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> statusCallback) {
        String uniqueId = UUID.randomUUID().toString();
        TreeQueryClusterRunner treeQueryClusterRunner = TreeQueryClusterRunnerImpl.builder().build();
        treeQueryClusterRunnerMap.put(uniqueId, treeQueryClusterRunner);

        Runnable runnableTask = () -> {
            treeQueryClusterRunner.runQueryTreeNetwork(node, statusCallback);
        };
        executor.execute(runnableTask);

        return uniqueId;
    }
}
