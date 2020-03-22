package org.treequery.model;

import org.apache.avro.Schema;

@FunctionalInterface
public interface AvroSchemaHelper {
    public Schema getAvroSchema(Node node);
}
