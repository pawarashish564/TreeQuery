package io.exp.treequery.execute.cache;

import org.apache.avro.generic.GenericRecord;

public interface CacheInputInterface {
    public GenericRecord getRetrievedValue(String identifier);
}
