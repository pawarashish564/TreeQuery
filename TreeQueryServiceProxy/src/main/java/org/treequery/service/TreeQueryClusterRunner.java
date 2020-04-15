package org.treequery.service;

import org.treequery.model.Node;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;

import java.io.Serializable;
import java.util.function.Consumer;

public interface TreeQueryClusterRunner extends Serializable {
    public void runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> StatusCallback);
    public void setTreeQueryClusterRunnerProxyInterface(TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface);
}
