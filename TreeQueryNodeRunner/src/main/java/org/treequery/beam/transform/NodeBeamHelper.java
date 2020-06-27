package org.treequery.beam.transform;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.treequery.model.Node;

import java.util.List;

public interface NodeBeamHelper {
    public PCollection<GenericRecord> apply(Pipeline pipeline, List<PCollection<GenericRecord> > parentCollectionLst, Node node);
}
