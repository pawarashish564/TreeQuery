package io.exp.treequery.model;




public interface MongoQueryAble extends DataSource {


    public QueryTypeEnum getQueryType() ;

    public String getDatabase() ;

    public String getCollection() ;

    public String getQuery() ;
}
