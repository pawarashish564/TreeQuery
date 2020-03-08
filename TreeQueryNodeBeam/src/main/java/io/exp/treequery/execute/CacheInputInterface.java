package io.exp.treequery.execute;

import org.apache.avro.generic.GenericRecord;

public interface CacheInputInterface {
    public GenericRecord getRetrievedValue(String identifier);
}
