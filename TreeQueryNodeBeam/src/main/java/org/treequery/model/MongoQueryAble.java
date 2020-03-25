package org.treequery.model;




public interface MongoQueryAble {

    public QueryTypeEnum getQueryType() ;

    public String getDatabase() ;

    public String getCollection() ;

    public String getQuery() ;
}
