package org.treequery.beam.cache;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;
import java.util.function.Consumer;

public interface BeamCacheOutputInterface {
    public void writeGenericRecord(PCollection<GenericRecord> stream, Schema avroSchema, String outputLabel);

    public Schema getPageRecord(long pageSize, long page, Consumer<GenericRecord> dataConsumer);
}
