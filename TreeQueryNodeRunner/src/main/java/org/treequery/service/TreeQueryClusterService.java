package org.treequery.service;

import org.treequery.model.Node;

import java.util.function.Consumer;

public interface TreeQueryClusterService {
    public String runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> StatusCallback);
}
