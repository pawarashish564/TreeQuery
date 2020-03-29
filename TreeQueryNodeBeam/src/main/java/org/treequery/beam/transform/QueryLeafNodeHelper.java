package org.treequery.beam.transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;
import org.treequery.Transform.QueryLeafNode;
import org.treequery.Transform.function.MongoQueryFunction;
import org.treequery.model.Node;

import java.util.List;
@Slf4j
public class QueryLeafNodeHelper implements NodeBeamHelper {
    @Override
    public PCollection<GenericRecord> apply(Pipeline pipeline, List<PCollection<GenericRecord>> parentCollectionLst, Node node) {
        if (!( node instanceof QueryLeafNode)){
            throw new IllegalArgumentException(String.format("%s is not Query Leaf Node", node.toString()));
        }
        if (parentCollectionLst.size() > 0){
            throw new IllegalArgumentException("Parent nodes should be empty for Query Input Node");
        }
        QueryLeafNode queryLeafNode = (QueryLeafNode) node;

        if (queryLeafNode.getQueryAble() instanceof MongoQueryFunction){
            //Do Mongo query
            return this.doMongoQuery(pipeline, queryLeafNode);
        }
        throw new NoSuchMethodError();
    }

    private PCollection<GenericRecord> doMongoQuery(Pipeline pipeline, QueryLeafNode queryLeafNode){
        PCollection<GenericRecord> output = null;

        if (!(queryLeafNode.getQueryAble() instanceof MongoQueryFunction)){
             throw new IllegalArgumentException("Queryable interface should be mongodb");
        }
        MongoQueryFunction mongoQueryFunction = (MongoQueryFunction)queryLeafNode.getQueryAble();
        PCollection<Document> mongoDocuments = pipeline.apply(
                MongoDbIO.read()
                .withDatabase(mongoQueryFunction.getDatabase())
                .withCollection(mongoQueryFunction.getCollection())
                .withUri(mongoQueryFunction.getMongoConnString())
        );
        Schema schema = queryLeafNode.getAvroSchemaObj();
        output = mongoDocuments.apply(ParDo.of(
            new DoFn<Document, GenericRecord>()   {
                @ProcessElement
                public void processElement(@Element Document document, OutputReceiver<GenericRecord > out) {
                    String jsonData = document.toJson();
                    log.debug(document.toJson());
                }
            }
            ));

        throw new IllegalArgumentException();
    }
}
