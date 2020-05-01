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
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;
import org.treequery.Transform.QueryLeafNode;
import org.treequery.Transform.function.MongoQueryFunction;
import org.treequery.Transform.function.SqlQueryFunction;

import org.treequery.beam.transform.query.JDBCReadTransform;
import org.treequery.beam.transform.query.MongoDocumentTransform;
import org.treequery.model.Node;
import org.treequery.model.QueryAble;

import java.io.IOException;
import java.sql.ResultSet;
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

        QueryAble queryAble = queryLeafNode.getQueryAble();
        if (queryAble instanceof MongoQueryFunction){
            return this.doMongoQuery(pipeline, queryLeafNode);
        }else if(queryAble instanceof SqlQueryFunction){
            return this.doSqlQuery(pipeline, queryLeafNode);
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

    private PCollection<GenericRecord> doSqlQuery(Pipeline pipeline, QueryLeafNode queryLeafNode){
        PCollection<GenericRecord> output=null;
        if (!(queryLeafNode.getQueryAble() instanceof SqlQueryFunction)){
            throw new IllegalArgumentException("Queryable interface should be SQL");
        }
        SqlQueryFunction sqlQueryFunction = (SqlQueryFunction)queryLeafNode.getQueryAble();
        //Reference : https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/io/jdbc/JdbcIO.html

        Schema schema = queryLeafNode.getAvroSchemaObj();
        pipeline.apply(
                JDBCReadTransform.getJDBCRead(sqlQueryFunction, schema)
        );
        return output;
    }

}
