package org.treequery.Transform.function;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.treequery.model.QueryAble;
import org.treequery.model.QueryTypeEnum;

@Getter
@Builder
public class SqlQueryFunction implements QueryAble {
    private final String database;
    private final String collection;
    private final String query;
    @NonNull
    private final String username;
    @NonNull
    private final String password;
    @NonNull
    private final String sqlConnString;
    @NonNull
    private final String driverClassName;

    @Override
    public QueryTypeEnum getQueryType() {
        return QueryTypeEnum.SQL;
    }

}
