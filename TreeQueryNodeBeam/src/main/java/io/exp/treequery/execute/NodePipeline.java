package io.exp.treequery.execute;

import io.exp.treequery.model.Node;

public interface NodePipeline {
    public void addNodeToPipeline(Node parentNode, Node node);
    public PipelineBuilderInterface getPipelineBuilder();
}
