package io.exp.treequery.beam;

import io.exp.treequery.execute.CacheInputInterface;
import org.apache.avro.generic.GenericRecord;

public class FileCacheInputImpl implements CacheInputInterface {
    String fileDirectory;

    @Override
    public GenericRecord getRetrievedValue(String identifier) {
        return null;
    }
}
