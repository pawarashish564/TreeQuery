package io.exp.treequery.execute;

import io.exp.treequery.model.Node;

public abstract class AbstractNodePipeline implements NodePipeline {
    @Override
    public NodePipeline addNodeToPipeline(Node parentNode, Node node) {
        return null;
    }
}
