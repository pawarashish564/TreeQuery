package org.treequery.Transform;

import lombok.NonNull;
import org.treequery.Transform.function.MongoQueryFunction;
import org.treequery.Transform.function.NoQueryFunction;
import org.treequery.model.*;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class QueryLeafNode extends Node implements DataSource {
    @NonNull
    String source;
    @NonNull
    String avro_schema;
    @NonNull
    QueryAble queryAble;

}
