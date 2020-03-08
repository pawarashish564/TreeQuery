package io.exp.treequery.execute;

import org.apache.avro.generic.GenericRecord;

public interface CacheIOInterface {
    public GenericRecord getRetrievedValue(String identifier);
}
