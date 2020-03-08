package io.exp.treequery.execute;

import io.exp.treequery.model.Node;

import java.util.List;

public interface PipelineBuilderInterface {
    public void buildPipeline(List<Node> parentNodeLst, Node node);
}
