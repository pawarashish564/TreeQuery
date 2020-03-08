package io.exp.treequery.beam;

import io.exp.treequery.execute.CacheIOInterface;
import org.apache.avro.generic.GenericRecord;

public class FileCacheIOImpl implements CacheIOInterface {
    String fileDirectory;

    @Override
    public GenericRecord getRetrievedValue(String identifier) {
        return null;
    }
}
