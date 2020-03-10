package io.exp.treequery.service;

import io.exp.treequery.model.Node;
import lombok.Builder;

@Builder
public class LocalSingleProcessTreeQueryClusterRunner implements TreeQueryClusterRunner {

    @Override
    public void runQueryTreeNetwork(Node node) {

    }
}
