package org.treequery.beam.cache;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

public class RedisCacheOutputImpl  implements BeamCacheOutputInterface {
    @Override
    public void writeGenericRecord(PCollection<GenericRecord> stream, Schema avroSchema, String outputLabel) {
        throw new NoSuchMethodError("Sorry, cache output not yet implemented!");
    }

    @Override
    public List<GenericRecord> getPageRecord(long pageSize, long page) {
        throw new NoSuchMethodError("Sorry, cache output not yet implemented!");
    }
}
