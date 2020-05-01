package org.treequery.beam.transform;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;
import org.treequery.Transform.QueryLeafNode;
import org.treequery.Transform.function.MongoQueryFunction;
import org.treequery.model.Node;

import java.io.IOException;
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
        PCollection<GenericRecord> output;

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
        output = mongoDocuments.apply( new MongoDocumentTransform(schema));

        return output;
    }


    private static class MongoDocumentTransform extends PTransform< PCollection<Document>, PCollection<GenericRecord> >{

        @NonNull
        Schema schema;
        MongoDocumentTransform(Schema schema){
            this.schema = schema;
        }

        @Override
        public PCollection<GenericRecord> expand(PCollection<Document> mongoDocuments) {
            AvroCoder coder = AvroCoder.of(GenericRecord.class, schema);
            PCollection<GenericRecord> genericRecords = mongoDocuments.apply(
                    ParDo.of(new ConversionFunction(this.schema))
            ).setCoder(coder);

            return genericRecords;
        }

        private static class ConversionFunction extends DoFn<Document, GenericRecord>{
            Schema schema;
            ConversionFunction(Schema schema){
                this.schema = schema;
            }
            @ProcessElement
            public void processElement(@Element Document document, OutputReceiver<GenericRecord > out) throws IOException {
                DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
                String jsonString = document.toJson();
                GenericRecord genericRecord = null;

                JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, jsonString);
                try {
                    genericRecord = datumReader.read(null, jsonDecoder);
                }catch(Throwable ex){
                    log.error(ex.getMessage());
                    log.error(jsonString);
                    throw new IllegalArgumentException(String.format("Failed to parse:%s", jsonString));
                }
                /*
                    log.error(ex.getMessage());
                    throw new IllegalStateException(ex.getMessage());
                }*/
                out.output(genericRecord);
            }
        }


    }


}
