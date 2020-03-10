package io.exp.treequery.beam;

import com.google.common.collect.Maps;
import io.exp.treequery.Transform.LoadLeafNode;
import io.exp.treequery.beam.transform.LoadLeafNodeHelper;
import io.exp.treequery.beam.transform.NodeBeamHelper;
import io.exp.treequery.execute.CacheInputInterface;
import io.exp.treequery.execute.PipelineBuilderInterface;
import io.exp.treequery.model.Node;
import lombok.Builder;
import lombok.Getter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;


import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


public class BeamPipelineBuilderImpl implements PipelineBuilderInterface {
    @Getter
    private final Pipeline pipeline = Pipeline.create();
    private Map<Node, PCollection<?>> nodePCollectionMap = Maps.newHashMap();

    @Override
    public void buildPipeline(List<Node> parentNodeLst, Node node) {
        NodeBeamHelper nodeBeamHelper = null;
        if ( node instanceof LoadLeafNode){
            nodeBeamHelper = new LoadLeafNodeHelper();
        }

        List<PCollection<?>> parentLst = parentNodeLst.stream().map(
            pNode->nodePCollectionMap.get(pNode)
        ).collect(Collectors.toList());

        PCollection<?> transform = nodeBeamHelper.apply(this.pipeline, parentLst, node);
        nodePCollectionMap.put(node, transform);
    }




}
