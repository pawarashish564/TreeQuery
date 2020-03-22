package org.treequery.Transform;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Lists;
import org.treequery.cluster.NodeFactory;
import org.treequery.model.ActionTypeEnum;
import org.treequery.model.JoinAble;
import org.treequery.model.Node;

import java.util.List;
import java.util.Optional;

public class TransformNodeFactory implements NodeFactory {

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

        JsonNode tmp = Optional.of(jsonNode.get("keys")).orElseThrow(()->new IllegalArgumentException("Inner Join keys missing"));
        if (tmp.isArray()){
            JoinNode.Key.KeyBuilder keyBuilder = JoinNode.Key.builder();
            ArrayNode arrayNode = (ArrayNode) tmp;
            arrayNode.forEach(
                    jCNode->{
                        keyBuilder.left(jCNode.get("left").asInt());
                        keyBuilder.right(jCNode.get("right").asInt());
                        JsonNode jNodeOn = Optional.of(jCNode.get("on")).orElseThrow(()->new IllegalArgumentException("Missing on in Inner join"));

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
                                                    .leftColumn(Optional.of(jOnChild.get("left")).orElseThrow(()->new IllegalArgumentException("Missing left in join")).asText())
                                                    .rightColumn(Optional.of(jOnChild.get("right")).orElseThrow(()->new IllegalArgumentException("Missing right in join")).asText())
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

        String source = Optional.of(jsonNode.get("source")).orElseThrow(()->new IllegalArgumentException("Missing source in Load Node")).asText();
        String avroSchema = Optional.of(jsonNode.get("avro_schema")).orElseThrow(()->new IllegalArgumentException("Missing avro_schema in Load Node")).asText();
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
