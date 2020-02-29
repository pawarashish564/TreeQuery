package io.exp.treequery.beam.Transform;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Lists;
import io.exp.treequery.cluster.Cluster;
import io.exp.treequery.cluster.NodeFactory;
import io.exp.treequery.cluster.NodeTreeFactory;
import io.exp.treequery.model.ActionTypeEnum;
import io.exp.treequery.model.JoinAble;
import io.exp.treequery.model.Node;

import java.util.List;
import java.util.Optional;

public class BeamNodeFactory implements NodeFactory {

    @Override
    public Node nodeFactoryMethod(JsonNode jsonNode) {
        ActionTypeEnum actionTypeEnum = ActionTypeEnum.valueOf(jsonNode.get("action").asText());
        Node node = null;
        switch(actionTypeEnum){
            case FLATTEN:
                node = createFlattenNode(jsonNode);
                break;
            case INNER_JOIN:
                node = createInnerJoinNode(jsonNode);
                break;
            case LOAD:
                node = createLoadNode(jsonNode);
                break;
            case QUERY:
                node = createQueryNode(jsonNode);
                break;
        }

        return node;
    }

    static Node createFlattenNode(JsonNode jsonNode){
        FlattenNode flattenNode = new FlattenNode();
        flattenNode.setBasicValue(jsonNode);
        return flattenNode;
    }
    static Node createInnerJoinNode(JsonNode jsonNode) {
        JoinNode joinNode = new JoinNode();
        joinNode.setBasicValue(jsonNode);

        JsonNode tmp = Optional.of(jsonNode.get("keys")).get();
        if (tmp.isArray()){
            JoinNode.Key.KeyBuilder keyBuilder = JoinNode.Key.builder();
            ArrayNode arrayNode = (ArrayNode) tmp;
            arrayNode.forEach(
                    jCNode->{
                        keyBuilder.left(jCNode.get("left").asInt());
                        keyBuilder.right(jCNode.get("right").asInt());
                        JsonNode jNodeOn = Optional.of(jCNode.get("on")).get();

                        List<JoinAble.KeyColumn> keyColumnList = Lists.newLinkedList();
                        if(!jNodeOn.isArray()){
                            throw new IllegalArgumentException("key join 'ON' should be array");
                        }
                        ArrayNode arrayOnNode = (ArrayNode) jNodeOn;
                        arrayOnNode.forEach(
                                jOnChild->{
                                    JoinAble.KeyColumn.KeyColumnBuilder keyColumnBuilder = JoinAble.KeyColumn.builder();
                                    keyColumnList.add(
                                            keyColumnBuilder
                                                    .leftColumn(Optional.of(jOnChild.get("left")).get().asText())
                                                    .rightColumn(Optional.of(jOnChild.get("right")).get().asText())
                                            .build()
                                    );
                                }
                        );
                        keyBuilder.columnLst(keyColumnList);
                        joinNode.keys.add(keyBuilder.build());
                    }
            );
        }
        return joinNode;
    }

    static Node createLoadNode(JsonNode jsonNode){
        LoadLeafNode loadLeafNode = null;
        LoadLeafNode.LoadLeafNodeBuilder loadLeafNodeBuilder = LoadLeafNode.builder();

        String source = Optional.of(jsonNode.get("source")).get().asText();
        String avroSchema = Optional.of(jsonNode.get("avro_schema")).get().asText();
        loadLeafNodeBuilder.source(source);
        loadLeafNodeBuilder.avro_schema(avroSchema);
        loadLeafNode = loadLeafNodeBuilder.build();
        loadLeafNode.setBasicValue(jsonNode);
        return loadLeafNode;
    }

    static Node createQueryNode(JsonNode jsonNode){
        NodeFactory dataSourceFactory = new DataSourceFactory();
        return dataSourceFactory.nodeFactoryMethod(jsonNode);
    }
}
