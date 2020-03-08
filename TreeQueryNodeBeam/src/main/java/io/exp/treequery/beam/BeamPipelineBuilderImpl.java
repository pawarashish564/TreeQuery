package io.exp.treequery.beam;

import com.google.common.collect.Maps;
import io.exp.treequery.execute.CacheInputInterface;
import io.exp.treequery.execute.PipelineBuilderInterface;
import io.exp.treequery.model.Node;
import lombok.Builder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;


import java.util.List;
import java.util.Map;


public class BeamPipelineBuilderImpl implements PipelineBuilderInterface {
    CacheInputInterface cacheInputInterface;
    private final Pipeline pipeline = Pipeline.create();
    private Map<Node, PCollection<?>> nodePCollectionMap = Maps.newHashMap();
    @Builder
    BeamPipelineBuilderImpl(CacheInputInterface cacheInputInterface){
        this.cacheInputInterface = cacheInputInterface;
    }


    @Override
    public void buildPipeline(List<Node> parentNodeLst, Node node) {

    }



}
