package io.exp.treequery.execute;

import io.exp.treequery.model.Node;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

public interface PipelineBuilderInterface {
    public void buildPipeline(List<Node> parentNodeLst, Node node);
    public PCollection<?> getPCollection(Node node);
    public Pipeline getPipeline();
}
