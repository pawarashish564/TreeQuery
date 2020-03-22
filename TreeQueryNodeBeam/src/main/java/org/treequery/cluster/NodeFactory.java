package org.treequery.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import org.treequery.model.Node;
@FunctionalInterface
public interface NodeFactory {
    Node nodeFactoryMethod(JsonNode jNode);
}
