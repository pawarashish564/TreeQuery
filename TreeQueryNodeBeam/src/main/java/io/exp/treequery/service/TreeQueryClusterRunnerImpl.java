package io.exp.treequery.service;

import io.exp.treequery.model.Node;
import lombok.Builder;

import java.util.function.Consumer;
import java.util.function.Supplier;

@Builder
public class TreeQueryClusterRunnerImpl implements TreeQueryClusterRunner {
    @Override
    public void runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> statusCallback) {

        statusCallback.accept(new StatusTreeQueryCluster());
    }
}
