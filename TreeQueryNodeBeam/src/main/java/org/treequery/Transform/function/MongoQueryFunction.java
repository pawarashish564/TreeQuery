package org.treequery.Transform.function;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.treequery.model.QueryAble;
import org.treequery.model.QueryTypeEnum;

@Getter
@Builder
public class MongoQueryFunction implements QueryAble {
    String database;
    String collection;
    String query;
    @NonNull
    String mongoConnString;

    @Override
    public QueryTypeEnum getQueryType() {
        return QueryTypeEnum.MONGO;
    }

}
