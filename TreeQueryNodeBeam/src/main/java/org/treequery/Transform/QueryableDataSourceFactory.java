package org.treequery.Transform;

import com.fasterxml.jackson.databind.JsonNode;
import org.treequery.Transform.function.MongoQueryFunction;
import org.treequery.Transform.function.SqlQueryFunction;
import org.treequery.cluster.NodeFactory;
import org.treequery.model.Node;
import org.treequery.model.QueryAble;
import org.treequery.model.QueryTypeEnum;
import org.treequery.utils.DatabaseSettingHelper;

import java.util.Optional;

public class QueryableDataSourceFactory implements NodeFactory {
    private DatabaseSettingHelper databaseSettingHelper = DatabaseSettingHelper.getDatabaseSettingHelper();

    @Override
    public Node nodeFactoryMethod(JsonNode jNode) {
        //Get queryType
        String queryType = Optional.ofNullable(jNode.get("queryType")).orElseThrow(()->new IllegalArgumentException("No query Type")).asText();
        QueryTypeEnum queryTypeEnum = QueryTypeEnum.valueOf(queryType);

        QueryAble queryAble = null;
        switch (queryTypeEnum){
            case MONGO:
                queryAble = createMongoLeadNode(jNode);
                break;
            case SQL:
                queryAble = createSqlLeadNode (jNode);
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
        queryLeafNodeBuilder.source(Optional.ofNullable(jNode.get("source")).orElseThrow(()->new IllegalArgumentException("Mongo source missing")).asText());
        queryLeafNodeBuilder.avro_schema(Optional.ofNullable(jNode.get("avro_schema")).orElseThrow(()->new IllegalArgumentException("Mongo avro schema missing")).asText());
        return queryLeafNodeBuilder;
    }

    private QueryAble createMongoLeadNode(JsonNode jNode){
        MongoQueryFunction.MongoQueryFunctionBuilder mongoFunctionBuilder = MongoQueryFunction.builder();
        mongoFunctionBuilder.database(Optional.ofNullable(jNode.get("database")).orElseThrow(()->new IllegalArgumentException("Mongo database missing")).asText());
        mongoFunctionBuilder.collection(Optional.ofNullable(jNode.get("collection")).orElseThrow(()->new IllegalArgumentException("Mongo collection missing")).asText());
        mongoFunctionBuilder.query(Optional.ofNullable(jNode.get("query")).map(q->q.asText()).orElse("{}"));
        mongoFunctionBuilder.mongoConnString(this.databaseSettingHelper.getMongoConnectionString());

        MongoQueryFunction mongoQueryFunction = mongoFunctionBuilder.build();

        return mongoQueryFunction;
    }

    private QueryAble createSqlLeadNode(JsonNode jNode){
        SqlQueryFunction.SqlQueryFunctionBuilder sqlQueryFunctionBuilder = SqlQueryFunction.builder();
        sqlQueryFunctionBuilder.database(Optional.ofNullable(
                jNode.get("database"))
                .orElseThrow(()->new IllegalArgumentException("SQL database missing"))
                .asText());
        sqlQueryFunctionBuilder.query(Optional.ofNullable(
                jNode.get("query"))
                .orElseThrow(()->new IllegalArgumentException("SQL query missing"))
                .asText());
        sqlQueryFunctionBuilder
                .username(this.databaseSettingHelper.getJDBCUser())
                .password(this.databaseSettingHelper.getJDBCPassword())
                .sqlConnString(this.databaseSettingHelper.getJDBCConnectionString())
                .driverClassName(this.databaseSettingHelper.getJDBCDriver());
        return sqlQueryFunctionBuilder.build();
    }
}
