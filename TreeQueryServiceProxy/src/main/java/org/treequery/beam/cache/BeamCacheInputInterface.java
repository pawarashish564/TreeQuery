package org.treequery.beam.cache;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

@FunctionalInterface
public interface BeamCacheInputInterface {
    public PCollection<GenericRecord> getRetrievedValue(Pipeline pipeline, String identifier);
}
