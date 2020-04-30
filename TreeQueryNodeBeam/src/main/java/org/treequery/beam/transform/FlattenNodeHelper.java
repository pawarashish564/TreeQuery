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
        PCollectionList<GenericRecord> genericRecordPCollectionList = null;
        try {
            genericRecordPCollectionList = PCollectionList.of(parentCollectionLst);
        }catch(NullPointerException ex){
            ex.printStackTrace();
            System.out.println(String.format("Number of elements:%d", parentCollectionLst.size()));
            int i = 0;
            for (PCollection<GenericRecord> strema: parentCollectionLst){
                System.out.println(String.format("%d:%s",++i, Optional.ofNullable(strema).map(s->s.toString()).orElse("null")));
            }
            throw new RuntimeException(ex.getMessage());
        }
        PCollection<GenericRecord> merged = genericRecordPCollectionList.apply(Flatten.<GenericRecord>pCollections());

        return merged;
    }
}
