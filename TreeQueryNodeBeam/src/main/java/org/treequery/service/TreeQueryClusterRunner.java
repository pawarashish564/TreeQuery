package org.treequery.service;

import org.treequery.model.Node;

import java.io.Serializable;
import java.util.function.Consumer;

public interface TreeQueryClusterRunner extends Serializable {
    public void runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> StatusCallback);
}
