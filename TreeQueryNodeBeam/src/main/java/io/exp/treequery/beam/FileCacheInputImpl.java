package io.exp.treequery.beam;

import io.exp.treequery.execute.CacheInputInterface;
import lombok.Builder;
import org.apache.avro.generic.GenericRecord;

@Builder
public class FileCacheInputImpl implements CacheInputInterface {
    String fileDirectory;

    @Override
    public GenericRecord getRetrievedValue(String identifier) {
        return null;
    }
}
