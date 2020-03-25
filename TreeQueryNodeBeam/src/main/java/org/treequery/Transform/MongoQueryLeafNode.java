package org.treequery.Transform;

import org.treequery.model.DataSource;
import org.treequery.model.Node;
import org.treequery.model.MongoQuery;
import org.treequery.model.QueryTypeEnum;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class MongoQueryLeafNode extends Node implements DataSource, MongoQuery {
    QueryTypeEnum queryType;
    String database;
    String collection;
    String query;
    String source;
    String avro_schema;




}
