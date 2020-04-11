package org.treequery.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.treequery.Transform.JoinNode;
import org.treequery.utils.AvroSchemaHelper;

import java.util.List;

public class BasicAvroSchemaHelperImpl implements AvroSchemaHelper {
    @Override
    public Schema getAvroSchema(Node node) {
        Schema.Parser parser = new Schema.Parser();
        String schemaJsonStr = this.getAvroSchemaJsonString(node);
        return parser.parse(schemaJsonStr);
    }

    @Override
    public String getAvroSchemaJsonString(Node node) {
        JsonNode schemaJson=null;
        try {
            schemaJson = this.__dfsHelper(node);
        }catch (Exception ex){
            throw new IllegalArgumentException(ex.getMessage());
        }
        return schemaJson.toString();
    }

    private JsonNode __dfsHelper(Node node) {
        ObjectMapper mapper = new ObjectMapper();

        if (node.getChildren().size() == 0) {
            if (! (node instanceof DataSource)){
                throw  new IllegalArgumentException(String.format("Node %s is not a datasource", node.getName()));
            }
            DataSource dataSource = (DataSource) node;
            String schema = dataSource.getAvro_schema();
            JsonNode jsonNode = null;
            //return avro schema
            try {
                jsonNode = mapper.readTree(schema);
            }catch (Exception ex){
                throw new IllegalArgumentException(ex.getMessage());
            }
            return jsonNode;
        }
        ObjectNode newSchema = mapper.createObjectNode();
        ArrayNode fieldArrayNode = mapper.createArrayNode();
        newSchema.set("fields", fieldArrayNode);
        List<JsonNode> schemaLst = Lists.newLinkedList();

        node.getChildren().forEach(
                child->{
                    JsonNode sch = this.__dfsHelper(child);
                    schemaLst.add(sch);
                }
        );
        if (node instanceof JoinNode){
            newSchema.put("name", node.getName());
            newSchema.put("type", "record");
            newSchema.put("namespace", "org.treequery.join");
            JoinAble joinFunction = ((JoinNode)node).getJoinFunction();
            joinFunction.getJoinKeys().forEach(
                    key->{
                        ObjectNode leftType = mapper.createObjectNode();
                        leftType.put("name", key.leftLabel);
                        leftType.set("type",schemaLst.get(key.leftInx));
                        fieldArrayNode.add(leftType);
                        ObjectNode rightType = mapper.createObjectNode();
                        rightType.put("name", key.rightLabel);
                        rightType.set("type", schemaLst.get(key.rightInx));
                        fieldArrayNode.add(rightType);
                    }
            );
        }else{
            if (schemaLst.size()>0){
                newSchema = (ObjectNode) schemaLst.get(0);
            }else{
                throw new IllegalArgumentException("No schema from child nodes");
            }
        }

        return newSchema;
    }
}
