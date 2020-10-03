package org.treequery.service;

import org.treequery.model.Node;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;

import java.util.function.Consumer;

public class RemoteProxyTreeQueryClusterRunner implements TreeQueryClusterRunner{

    TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;

    @Override
    public void runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> StatusCallback) {
        treeQueryClusterRunnerProxyInterface.runQueryTreeNetwork(
                node, StatusCallback
        );
    }

    @Override
    public void setTreeQueryClusterRunnerProxyInterface(TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface) {
        throw new NoSuchMethodError("Not supported for new LocalTreeQueryClusterRunner");
    }
}
