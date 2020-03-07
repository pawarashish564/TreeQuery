package io.exp.treequery.Transform;

import com.fasterxml.jackson.databind.JsonNode;
import io.exp.treequery.cluster.NodeFactory;
import io.exp.treequery.model.Node;
import io.exp.treequery.model.QueryTypeEnum;

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
        mongoQueryLeafNodeBuilder.database(Optional.of(jNode.get("database")).get().asText());
        mongoQueryLeafNodeBuilder.source(Optional.of(jNode.get("source")).get().asText());
        mongoQueryLeafNodeBuilder.collection(Optional.of(jNode.get("collection")).get().asText());
        mongoQueryLeafNodeBuilder.query(Optional.ofNullable(jNode.get("query")).map(q->q.asText()).orElse("{}"));
        mongoQueryLeafNodeBuilder.avro_schema(Optional.ofNullable(jNode.get("avro_schema")).get().asText());

        MongoQueryLeafNode mongoQueryLeafNode = mongoQueryLeafNodeBuilder.build();
        mongoQueryLeafNode.setBasicValue(jNode);
        return mongoQueryLeafNode;
    }
}
