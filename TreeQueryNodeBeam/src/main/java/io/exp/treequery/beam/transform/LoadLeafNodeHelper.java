package io.exp.treequery.beam.transform;

import io.exp.treequery.Transform.LoadLeafNode;
import io.exp.treequery.model.Node;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

public class LoadLeafNodeHelper implements NodeBeamHelper{
    @Override
    public PCollection<?> apply(Pipeline pipeline, List<PCollection<?>> parentCollectionLst, Node node) {
        if (!( node instanceof LoadLeafNode)){
            throw new IllegalArgumentException(String.format("%s is not Leaf Avro File Node", node.getDescription()));
        }
        if (parentCollectionLst.size() > 0){
            throw new IllegalArgumentException("Parent nodes should be empty for Leaf Avro File Node");
        }
        LoadLeafNode loadLeafNode = (LoadLeafNode) node;
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(loadLeafNode.getAvro_schema());
        PCollection<GenericRecord> avroDocuments = pipeline.apply(AvroIO.readGenericRecords(schema).from(loadLeafNode.getSource()));

        return avroDocuments;
    }
}
