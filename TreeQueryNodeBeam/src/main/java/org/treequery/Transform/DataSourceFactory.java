package org.treequery.Transform;

import com.fasterxml.jackson.databind.JsonNode;
import org.treequery.cluster.NodeFactory;
import org.treequery.model.Node;
import org.treequery.model.QueryTypeEnum;

import java.util.Optional;

public class DataSourceFactory implements NodeFactory {
    @Override
    public Node nodeFactoryMethod(JsonNode jNode) {
        //Get queryType
        String queryType = Optional.of(jNode.get("queryType")).get().asText();
        QueryTypeEnum queryTypeEnum = QueryTypeEnum.valueOf(queryType);
        Node node = null;
        switch (queryTypeEnum){
            case MONGO:
                node = createMongoLeadNode(jNode);
                break;
        }

        return node;
    }

    private Node createMongoLeadNode(JsonNode jNode){
        MongoQueryLeafNode.MongoQueryLeafNodeBuilder mongoQueryLeafNodeBuilder = MongoQueryLeafNode.builder();

        mongoQueryLeafNodeBuilder.queryType(QueryTypeEnum.MONGO);
        mongoQueryLeafNodeBuilder.database(Optional.of(jNode.get("database")).orElseThrow(()->new IllegalArgumentException("Mongo database missing")).asText());
        mongoQueryLeafNodeBuilder.source(Optional.of(jNode.get("source")).orElseThrow(()->new IllegalArgumentException("Mongo source missing")).asText());
        mongoQueryLeafNodeBuilder.collection(Optional.of(jNode.get("collection")).orElseThrow(()->new IllegalArgumentException("Mongo collection missing")).asText());
        mongoQueryLeafNodeBuilder.query(Optional.ofNullable(jNode.get("query")).map(q->q.asText()).orElse("{}"));
        mongoQueryLeafNodeBuilder.avro_schema(Optional.ofNullable(jNode.get("avro_schema")).orElseThrow(()->new IllegalArgumentException("Mongo avro schema missing")).asText());

        MongoQueryLeafNode mongoQueryLeafNode = mongoQueryLeafNodeBuilder.build();
        mongoQueryLeafNode.setBasicValue(jNode);
        return mongoQueryLeafNode;
    }
}
