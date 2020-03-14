package io.exp.treequery.execute.cache;



import lombok.Builder;
import org.apache.avro.generic.GenericRecord;

@Builder
public class FileCacheInputImpl implements CacheInputInterface {
    String fileDirectory;

    @Override
    public Object getRetrievedValue(String identifier) {
        return null;
    }

    @Override
    public String getSchema(String identifier) {
        return null;
    }
}
