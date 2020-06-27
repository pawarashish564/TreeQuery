package org.treequery.beam.transform.query;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;

import java.io.IOException;

@Slf4j
public class MongoDocumentTransform extends PTransform<PCollection<Document>, PCollection<GenericRecord> > {

    @NonNull
    Schema schema;

    public MongoDocumentTransform(Schema schema) {
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

    private static class ConversionFunction extends DoFn<Document, GenericRecord> {
        Schema schema;

        ConversionFunction(Schema schema) {
            this.schema = schema;
        }

        @ProcessElement
        public void processElement(@Element Document document, OutputReceiver<GenericRecord> out) throws IOException {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            String jsonString = document.toJson();
            GenericRecord genericRecord = null;

            JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, jsonString);
            try {
                genericRecord = datumReader.read(null, jsonDecoder);
            } catch (Throwable ex) {
                log.error(ex.getMessage());
                log.error(jsonString);
                throw new IllegalArgumentException(String.format("Failed to parse:%s", jsonString));
            }
            out.output(genericRecord);
        }
    }
}
