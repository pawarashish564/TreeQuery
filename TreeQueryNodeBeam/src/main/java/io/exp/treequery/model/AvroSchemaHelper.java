package io.exp.treequery.model;

import org.apache.avro.Schema;

public interface AvroSchemaHelper {
    public Schema getAvroSchema(Node node);
}
