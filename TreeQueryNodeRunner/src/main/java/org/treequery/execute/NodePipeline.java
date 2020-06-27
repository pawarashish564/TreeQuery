package org.treequery.execute;

import org.treequery.model.Node;

public interface NodePipeline {
    public void addNodeToPipeline(Node parentNode, Node node);
    public PipelineBuilderInterface getPipelineBuilder();
}
