package io.exp.treequery.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import io.exp.treequery.model.Node;
@FunctionalInterface
public interface NodeFactory {
    Node nodeFactoryMethod(JsonNode jNode);
}
