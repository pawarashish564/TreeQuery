package org.treequery.Transform;

import org.apache.beam.repackaged.direct_java.runners.core.construction.graph.QueryablePipeline;
import org.treequery.Transform.function.MongoQueryFunction;
import org.treequery.Transform.function.NoQueryFunction;
import org.treequery.model.*;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class QueryLeafNode extends Node implements DataSource {
    String source;
    String avro_schema;
    QueryAble queryAble;


    @Override
    public  QueryAble getQueryFunction(){
        return queryAble;
    }
}
