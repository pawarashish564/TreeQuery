package org.treequery.beam;

import com.google.common.collect.Maps;
import org.apache.beam.sdk.coders.AvroCoder;
import org.treequery.Transform.LoadLeafNode;
import org.treequery.Transform.QueryLeafNode;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.beam.transform.LoadLeafNodeHelper;
import org.treequery.beam.transform.NodeBeamHelper;
import org.treequery.beam.transform.QueryLeafNodeHelper;
import org.treequery.execute.PipelineBuilderInterface;
import org.treequery.model.AvroSchemaHelper;
import org.treequery.model.Node;
import lombok.Builder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.values.PCollection;


import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


public class BeamPipelineBuilderImpl implements PipelineBuilderInterface {

    private final Pipeline pipeline = Pipeline.create();
    private Map<Node, PCollection<GenericRecord>> nodePCollectionMap = Maps.newHashMap();
    private BeamCacheOutputInterface beamCacheOutputInterface;
    private AvroSchemaHelper avroSchemaHelper;
    private Node __node = null;

    @Builder
    public BeamPipelineBuilderImpl(BeamCacheOutputInterface beamCacheOutputInterface, AvroSchemaHelper avroSchemaHelper){
        this.beamCacheOutputInterface = beamCacheOutputInterface;
        this.avroSchemaHelper = avroSchemaHelper;
    }

    private NodeBeamHelper prepareNodeBeamHelper(Node node){
        NodeBeamHelper nodeBeamHelper = null;
        if ( node instanceof LoadLeafNode){
            nodeBeamHelper = new LoadLeafNodeHelper();
        }
        else if (node instanceof QueryLeafNode){
            nodeBeamHelper = new QueryLeafNodeHelper();
        }
        return nodeBeamHelper;
    }

    @Override
    public void buildPipeline(List<Node> parentNodeLst, Node node) {
        NodeBeamHelper nodeBeamHelper = prepareNodeBeamHelper(node);
        this.__node = node;

        List<PCollection<GenericRecord>> parentLst = parentNodeLst.stream().map(
            pNode->nodePCollectionMap.get(pNode)
        ).collect(Collectors.toList());

        PCollection<GenericRecord> transform = nodeBeamHelper.apply(this.pipeline, parentLst, node);
        nodePCollectionMap.put(node, transform);
    }

    @Override
    public boolean executePipeline() {
        String hashCode = __node.getIdentifier();
        Pipeline pipeline = this.pipeline;
        //Final result Pcollection
        PCollection<GenericRecord> record = this.getPCollection(__node);
        Schema avroSchema = Optional.of(avroSchemaHelper).orElseThrow(()->new IllegalArgumentException("Missing Avro Schema Helper")).getAvroSchema(__node);
        Optional.ofNullable(avroSchema).orElseThrow(()->new IllegalArgumentException(String.format("Schema of %s should not be null",__node.getDescription())));
        this.beamCacheOutputInterface.writeGenericRecord(record, avroSchema, hashCode);
        //Most simple runner
        PipelineResult.State state = pipeline.run().waitUntilFinish();

        return true;
    }

    PCollection<GenericRecord> getPCollection(Node node) {
        return Optional.of(nodePCollectionMap.get(node)).get();
    }
    Pipeline getPipeline(){
        return this.pipeline;
    }
}
