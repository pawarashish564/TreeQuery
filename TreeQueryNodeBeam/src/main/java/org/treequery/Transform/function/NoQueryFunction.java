package org.treequery.Transform.function;

import org.treequery.model.QueryAble;
import org.treequery.model.QueryTypeEnum;

public class NoQueryFunction implements QueryAble {
    @Override
    public QueryTypeEnum getQueryType() {
        return QueryTypeEnum.NONE;
    }

    @Override
    public String getQuery() {
        throw new IllegalStateException("Query not supported");
    }
}
