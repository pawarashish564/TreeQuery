package io.exp.treequery.execute.cache;

import org.apache.avro.generic.GenericRecord;

public interface CacheOutputInterface {
    public void writeGenericRecord(GenericRecord record);
}
