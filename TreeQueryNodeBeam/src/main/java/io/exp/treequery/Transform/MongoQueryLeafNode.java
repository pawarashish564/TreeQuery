package io.exp.treequery.Transform;

import io.exp.treequery.model.Node;
import io.exp.treequery.model.MongoQueryAble;
import io.exp.treequery.model.QueryTypeEnum;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class MongoQueryLeafNode extends Node implements MongoQueryAble {
    QueryTypeEnum queryType;
    String database;
    String collection;
    String query;
    String source;
    String avro_schema;




}
