package io.exp.treequery.beam.cache;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

public interface BeamCacheInputInterface {
    public PCollection<GenericRecord> getRetrievedValue(Pipeline pipeline, String identifier);
}
