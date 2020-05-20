package org.treequery.beam;

import com.google.common.collect.Maps;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.treequery.Transform.FlattenNode;
import org.treequery.Transform.JoinNode;
import org.treequery.Transform.LoadLeafNode;
import org.treequery.Transform.QueryLeafNode;
import org.treequery.beam.cache.BeamCacheOutputInterface;
import org.treequery.beam.transform.*;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.exception.NotAllChildBeamReadyToAttach;
import org.treequery.execute.PipelineBuilderInterface;
import org.treequery.model.CacheNode;
import org.treequery.utils.AvroSchemaHelper;
import org.treequery.model.Node;
import lombok.Builder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.values.PCollection;
import org.treequery.beam.cache.CacheInputInterface;


import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class BeamPipelineBuilderImpl implements PipelineBuilderInterface {

    private final Pipeline pipeline = Pipeline.create();
    private Map<Node, PCollection<GenericRecord>> nodePCollectionMap = Maps.newHashMap();
    @NonNull
    private BeamCacheOutputInterface beamCacheOutputInterface;
    @NonNull
    private AvroSchemaHelper avroSchemaHelper;
    @NonNull
    private final TreeQuerySetting treeQuerySetting;
    @NonNull
    private final CacheInputInterface cacheInputInterface;

    private DiscoveryServiceInterface discoveryServiceInterface;
    private Node __node = null;

    @Builder
    public BeamPipelineBuilderImpl(BeamCacheOutputInterface beamCacheOutputInterface,
                                   AvroSchemaHelper avroSchemaHelper,
                                   DiscoveryServiceInterface discoveryServiceInterface,
                                   CacheInputInterface cacheInputInterface,
                                   TreeQuerySetting treeQuerySetting){
        this.beamCacheOutputInterface = beamCacheOutputInterface;
        this.avroSchemaHelper = avroSchemaHelper;
        this.discoveryServiceInterface = discoveryServiceInterface;
        this.cacheInputInterface = cacheInputInterface;
        this.treeQuerySetting = treeQuerySetting;
    }

    private NodeBeamHelper prepareNodeBeamHelper(Node node){
        NodeBeamHelper nodeBeamHelper = null;
        if ( node instanceof LoadLeafNode){
            nodeBeamHelper = new LoadLeafNodeHelper();
        }
        else if (node instanceof QueryLeafNode){
            nodeBeamHelper = new QueryLeafNodeHelper();
        }
        else if (node instanceof JoinNode){
            nodeBeamHelper = new JoinNodeHelper(avroSchemaHelper);
        }
        else if (node instanceof CacheNode){
            nodeBeamHelper =  CacheBeamHelper.builder()
                    .treeQuerySetting(this.treeQuerySetting)
                    .cacheInputInterface(cacheInputInterface)
                    .discoveryServiceInterface(discoveryServiceInterface).build();
        }
        else if (node instanceof FlattenNode){
            nodeBeamHelper = new FlattenNodeHelper();
        }
        else{
            log.error("Not support node transforming to Apache Beam:",node.toString());
            throw new NoSuchMethodError(String.format("Not support node transforming to Apache Beam:%s",node.toString()));
        }
        return nodeBeamHelper;
    }

    @Override
    public void buildPipeline(List<Node> parentNodeLst, Node node) {
        NodeBeamHelper nodeBeamHelper = prepareNodeBeamHelper(node);
        this.__node = node;

        List<PCollection<GenericRecord>> parentLst = parentNodeLst.stream().map(
            pNode-> {
                PCollection<GenericRecord> beamPipeline =  Optional.ofNullable(nodePCollectionMap.get(pNode))
                        .orElseThrow(()-> new NotAllChildBeamReadyToAttach(pNode));
                log.debug(String.format("insertNode2PipelineHelper insert %s to parents %s", node.getName(),pNode.getName()));
                return beamPipeline;
            }
        ).collect(Collectors.toList());

        PCollection<GenericRecord> transform = nodeBeamHelper.apply(this.pipeline, parentLst, node);

        log.debug(String.format("Node %s insert beam transform with NodeBeamHelper %s",
                node.getName(), nodeBeamHelper.getClass().getName()));
        nodePCollectionMap.put(node, transform);
    }

    @Override
    public boolean executePipeline() {
        String hashCode = __node.getIdentifier();
        Pipeline pipeline = this.pipeline;
        //Final result Pcollection
        PCollection<GenericRecord> record = this.getPCollection(__node);
        Schema avroSchema = Optional.ofNullable(avroSchemaHelper).orElseThrow(()->new IllegalArgumentException("Missing Avro Schema Helper")).getAvroSchema(__node);
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
