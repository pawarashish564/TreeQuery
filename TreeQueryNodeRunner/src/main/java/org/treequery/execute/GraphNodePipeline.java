package org.treequery.execute;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.treequery.cluster.Cluster;
import org.treequery.exception.NotAllChildBeamReadyToAttach;
import org.treequery.utils.AvroSchemaHelper;
import org.treequery.model.CacheNode;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

@Slf4j
public  class GraphNodePipeline implements NodePipeline {
    Cluster cluster;
    PipelineBuilderInterface pipelineBuilderInterface;
    AvroSchemaHelper avroSchemaHelper;
    CacheTypeEnum cacheTypeEnum;
    Map<Node, List> graph = Maps.newHashMap();
    Map<Node, List> depends = Maps.newHashMap();

    @Builder
    GraphNodePipeline(Cluster cluster, PipelineBuilderInterface pipelineBuilderInterface,AvroSchemaHelper avroSchemaHelper, CacheTypeEnum cacheTypeEnum) {
        this.cluster = cluster;
        this.pipelineBuilderInterface = pipelineBuilderInterface;
        this.avroSchemaHelper = Optional.ofNullable(avroSchemaHelper).orElseThrow(()->new IllegalArgumentException("Avro Schema Helper not null"));
        this.cacheTypeEnum = cacheTypeEnum;
    }



    @Override
    public void addNodeToPipeline(Node parentNode, Node node) {
        Node newParentNode = null;
        if (node == null){
            this.graph.put(parentNode, Lists.newLinkedList());
            return ;
        }

        if (parentNode.getCluster().equals(node.getCluster())){
            newParentNode = parentNode;
        }else{
            CacheNode cacheNode = CacheNode
                    .builder()
                    .node(parentNode)
                    .cacheTypeEnum(cacheTypeEnum)
                    .avroSchemaHelper(avroSchemaHelper)
                    .build();

            assert (cacheNode.equals(parentNode));
            newParentNode = cacheNode;
        }
        this.helpGetDefaultMapValue(this.graph, newParentNode).add(node);
        this.helpGetDefaultMapValue(this.depends, node).add(newParentNode);
        log.debug(String.format("Pipeline Add %s to %s", node.getName(), newParentNode.getName()));
        log.debug(String.format("Dependency map: %s depends on %s", node.getName(), newParentNode.getName()));
        assert(newParentNode!=null);

        return ;
    }
    @Override
    public PipelineBuilderInterface getPipelineBuilder(){
        Queue<Node> queue = Queues.newLinkedBlockingDeque();
        //Fill in blank dependency for root
        this.graph.keySet().forEach(
                rNode->{
                    List<Node> dependOn = helpGetDefaultMapValue(this.depends, rNode);
                    if (dependOn.size()==0){
                        queue.add(rNode);
                        log.debug(String.format("Enqueue %s for processing", rNode.getName()));
                    }
                }
        );
        while (queue.size()>0){
            Node node = queue.remove();
            List<Node> dependOnList = this.depends.get(node);

            try {
                this.insertNode2PipelineHelper(dependOnList, node);
            }catch(NotAllChildBeamReadyToAttach nac){
                log.debug(nac.getMessage());
                if (queue.size()==0){
                    throw nac;
                }
                queue.add(node);
                continue;
            }

            List<Node> nextChildLst = this.graph.get(node);
            nextChildLst.forEach(
                    c->{
                        log.debug(String.format("Check insert child node {%s} to beam", c.getName()));
                        if (!queue.contains(c)){
                            log.debug(String.format("add child node {%s} to beam", c.getName()));
                            queue.add(c);
                        }
                    }
            );

        }
        return this.pipelineBuilderInterface;
    }

    private void insertNode2PipelineHelper(List<Node> parentList, Node node){
        pipelineBuilderInterface.buildPipeline(parentList, node);
    }


    private List helpGetDefaultMapValue(Map<Node, List> m, Node key){
        m.putIfAbsent(key, Lists.newLinkedList());
        return m.get(key);
    }


}
