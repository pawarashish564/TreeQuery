package io.exp.treequery.execute;

import io.exp.treequery.model.Node;

public interface NodePipeline {
    public NodePipeline addNodeToPipeline(Node parentNode, Node node);
}
