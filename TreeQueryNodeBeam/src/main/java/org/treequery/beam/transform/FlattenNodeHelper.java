package org.treequery.beam.transform;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.treequery.exception.FlattenRunTimeException;
import org.treequery.model.Node;

import java.util.List;
import java.util.Optional;

public class FlattenNodeHelper implements NodeBeamHelper {
    @Override
    public PCollection<GenericRecord> apply(Pipeline pipeline, List<PCollection<GenericRecord>> parentCollectionLst, Node node) {
        if (parentCollectionLst.size() < 1){
            throw new FlattenRunTimeException("Flatten element size should be >= 1");
        }
        PCollectionList<GenericRecord> genericRecordPCollectionList = PCollectionList.of(parentCollectionLst);

        PCollection<GenericRecord> merged = genericRecordPCollectionList.apply(Flatten.<GenericRecord>pCollections());

        return merged;
    }
}
