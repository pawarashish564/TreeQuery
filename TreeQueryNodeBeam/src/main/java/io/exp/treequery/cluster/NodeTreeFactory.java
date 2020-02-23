package io.exp.treequery.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.exp.treequery.model.Node;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.data.Json;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Stack;
import java.util.stream.Stream;

@Slf4j
@Builder
public class NodeTreeFactory {
    private final NodeFactory nodeFactory;

    public  Node parseJsonFile (String jsonFile)  {
        Node output = null;
        StringBuilder contentBuilder = new StringBuilder();
        try(Stream<String> stream = Files.lines( Paths.get(jsonFile), StandardCharsets.UTF_8)){
            stream.forEach(s -> contentBuilder.append(s));
            String jsonString = contentBuilder.toString();
            output = parseJsonString(jsonString);
        }catch(IOException ioe){
            log.error(ioe.getMessage());
        }
        return output;
    }

    public  Node parseJsonString(String jsonInputString) throws JsonProcessingException {
        Node rootNode=null;


        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonInputString);

        Stack<JNodeInsert> stack = new Stack<>();
        stack.push(new JNodeInsert(actualObj, null));

        while (stack.size() > 0){
            JNodeInsert jNodeInsert = stack.pop();
            JsonNode jNode = jNodeInsert.getJsonNode();
            Node parentNode = jNodeInsert.getParent();
            Node newNode = nodeFactory.nodeFactoryMethod(jNode);
            if (rootNode == null){
                rootNode = newNode;
            }else{
                parentNode.insertChildren(newNode);
            }

            if (jNode.hasNonNull("children") ){
                JsonNode tmp = jNode.get("children");
                if (tmp.isArray()){
                    ArrayNode arrayNode = (ArrayNode) tmp;
                    arrayNode.forEach(
                            jCNode->{
                                stack.push(new JNodeInsert(jCNode, newNode));
                            }
                    );
                }
            }
        }

        return rootNode;
    }

    @RequiredArgsConstructor
    @Getter
    private static class JNodeInsert{
        private final JsonNode jsonNode;
        private final Node parent;
    }
}
