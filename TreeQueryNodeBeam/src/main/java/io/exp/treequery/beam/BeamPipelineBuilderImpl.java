package io.exp.treequery.beam;

import io.exp.treequery.execute.CacheIOInterface;
import io.exp.treequery.execute.PipelineBuilderInterface;
import io.exp.treequery.model.Node;
import lombok.Builder;

import java.util.List;

@Builder
public class BeamPipelineBuilderImpl implements PipelineBuilderInterface {
    CacheIOInterface outputIOInterface;

    @Override
    public void buildPipeline(List<Node> parentNodeLst, Node node) {

    }
}
