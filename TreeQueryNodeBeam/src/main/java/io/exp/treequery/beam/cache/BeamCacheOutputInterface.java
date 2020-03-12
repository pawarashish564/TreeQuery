package io.exp.treequery.beam.cache;

import io.exp.treequery.model.Node;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.PCollection;

public interface BeamCacheOutputInterface {
    public void writeGenericRecord(Node node, String outputLabel);
}
