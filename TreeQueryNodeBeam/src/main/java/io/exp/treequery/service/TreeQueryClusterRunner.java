package io.exp.treequery.service;

import io.exp.treequery.model.Node;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface TreeQueryClusterRunner extends Serializable {
    public void runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> StatusCallback);
}
