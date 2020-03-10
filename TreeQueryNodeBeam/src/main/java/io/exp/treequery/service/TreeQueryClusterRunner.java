package io.exp.treequery.service;

import io.exp.treequery.model.Node;

import java.io.Serializable;

public interface TreeQueryClusterRunner extends Serializable {
    public void runQueryTreeNetwork(Node node);
}
