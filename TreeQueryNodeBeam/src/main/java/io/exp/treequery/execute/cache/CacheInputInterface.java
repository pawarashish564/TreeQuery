package io.exp.treequery.execute.cache;

public interface CacheInputInterface {
    public Object getRetrievedValue(String identifier);
    public String getSchema(String identifier);
}
