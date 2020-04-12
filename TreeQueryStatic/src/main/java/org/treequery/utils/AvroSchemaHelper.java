package org.treequery.utils;

import org.apache.avro.Schema;
import org.treequery.model.Node;

public interface AvroSchemaHelper {
    public Schema getAvroSchema(Node node);
    public String getAvroSchemaJsonString(Node node);
}
