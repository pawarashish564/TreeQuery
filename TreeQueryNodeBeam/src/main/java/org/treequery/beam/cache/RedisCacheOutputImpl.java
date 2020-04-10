package org.treequery.beam.cache;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.PCollection;

public class RedisCacheOutputImpl  implements BeamCacheOutputInterface {
    @Override
    public void writeGenericRecord(PCollection<GenericRecord> stream, Schema avroSchema, String outputLabel) {
        throw new NoSuchMethodError("Sorry, cache output not yet implemented!");
    }
}
