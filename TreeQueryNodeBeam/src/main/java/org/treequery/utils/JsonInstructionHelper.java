package org.treequery.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.treequery.Transform.TransformNodeFactory;
import org.treequery.cluster.NodeFactory;
import org.treequery.cluster.NodeTreeFactory;
import org.treequery.model.Node;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

@Slf4j
public class JsonInstructionHelper {

    public static  String parseJsonFile (String jsonFile)  {
        String jsonString="";
        StringBuilder contentBuilder = new StringBuilder();
        try(Stream<String> stream = Files.lines( Paths.get(jsonFile), StandardCharsets.UTF_8)){
            stream.forEach(s -> contentBuilder.append(s));
            jsonString= contentBuilder.toString();
        }catch(IOException ioe){
            log.error(ioe.getMessage());
        }
        return jsonString;
    }

    public static Node createNode(String jsonString) throws JsonProcessingException {
        Node rootNode = null;
        NodeFactory nodeFactory;
        NodeTreeFactory nodeTreeFactory;
        nodeFactory = new TransformNodeFactory();
        nodeTreeFactory = NodeTreeFactory.builder().nodeFactory(nodeFactory).build();
        rootNode = nodeTreeFactory.parseJsonString(jsonString);
        return rootNode;
    }
}
