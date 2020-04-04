package org.treequery.Transform;

import com.fasterxml.jackson.databind.JsonNode;
import org.treequery.Transform.function.MongoQueryFunction;
import org.treequery.cluster.NodeFactory;
import org.treequery.model.Node;
import org.treequery.model.QueryAble;
import org.treequery.model.QueryTypeEnum;

import java.util.Optional;

public class QueryableDataSourceFactory implements NodeFactory {
    private static String MongoDBQueryString = "mongodb://mongoadmin:secret@localhost:27017";
    @Override
    public Node nodeFactoryMethod(JsonNode jNode) {
        //Get queryType
        String queryType = Optional.of(jNode.get("queryType")).get().asText();
        QueryTypeEnum queryTypeEnum = QueryTypeEnum.valueOf(queryType);

        QueryAble queryAble = null;
        switch (queryTypeEnum){
            case MONGO:
                queryAble = createMongoLeadNode(jNode);
                break;
        }
        QueryLeafNode.QueryLeafNodeBuilder queryLeafNodeBuilder = QueryLeafNode.builder();
        queryLeafNodeBuilder = this.fillBasicInfo(queryLeafNodeBuilder, jNode);
        queryLeafNodeBuilder.queryAble(queryAble);

        Node node = queryLeafNodeBuilder.build();
        node.setBasicValue(jNode);
        return node;
    }

    private QueryLeafNode.QueryLeafNodeBuilder fillBasicInfo(QueryLeafNode.QueryLeafNodeBuilder queryLeafNodeBuilder, JsonNode jNode){
        queryLeafNodeBuilder.source(Optional.of(jNode.get("source")).orElseThrow(()->new IllegalArgumentException("Mongo source missing")).asText());
        queryLeafNodeBuilder.avro_schema(Optional.ofNullable(jNode.get("avro_schema")).orElseThrow(()->new IllegalArgumentException("Mongo avro schema missing")).asText());
        return queryLeafNodeBuilder;
    }

    private QueryAble createMongoLeadNode(JsonNode jNode){
        MongoQueryFunction.MongoQueryFunctionBuilder mongoFunctionBuilder = MongoQueryFunction.builder();
        mongoFunctionBuilder.database(Optional.of(jNode.get("database")).orElseThrow(()->new IllegalArgumentException("Mongo database missing")).asText());
        mongoFunctionBuilder.collection(Optional.of(jNode.get("collection")).orElseThrow(()->new IllegalArgumentException("Mongo collection missing")).asText());
        mongoFunctionBuilder.query(Optional.ofNullable(jNode.get("query")).map(q->q.asText()).orElse("{}"));
        mongoFunctionBuilder.mongoConnString(MongoDBQueryString);

        MongoQueryFunction mongoQueryFunction = mongoFunctionBuilder.build();

        return mongoQueryFunction;
    }
}
