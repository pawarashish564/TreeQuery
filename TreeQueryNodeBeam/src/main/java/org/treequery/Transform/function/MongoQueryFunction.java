package org.treequery.Transform.function;

import lombok.Builder;
import lombok.Getter;
import org.treequery.model.QueryAble;
import org.treequery.model.QueryTypeEnum;

@Getter
@Builder
public class MongoQueryFunction implements QueryAble {
    String database;
    String collection;
    String query;

    @Override
    public QueryTypeEnum getQueryType() {
        return QueryTypeEnum.MONGO;
    }

}
