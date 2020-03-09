package io.exp.treequery.beam.transform;

import io.exp.treequery.model.Node;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

public interface NodeBeamHelper {
    public PCollection<?> apply(Pipeline pipeline, List<PCollection<?> > parentCollectionLst, Node node);
}
