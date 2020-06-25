package org.treequery.execute;

import org.treequery.model.Node;

import java.util.List;

public interface PipelineBuilderInterface {
    public void buildPipeline(List<Node> parentNodeLst, Node node);
    public boolean executePipeline();
}
