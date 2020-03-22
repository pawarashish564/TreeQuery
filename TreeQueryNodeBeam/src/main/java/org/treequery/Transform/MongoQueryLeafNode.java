package org.treequery.Transform;

import org.treequery.model.Node;
import org.treequery.model.MongoQueryAble;
import org.treequery.model.QueryTypeEnum;
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
