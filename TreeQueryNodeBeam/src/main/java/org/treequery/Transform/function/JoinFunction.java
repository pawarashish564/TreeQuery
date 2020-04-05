package org.treequery.Transform.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.NonNull;
import org.treequery.model.JoinAble;
import org.treequery.model.JoinTypeEnum;

import java.util.List;
import java.util.Optional;

@Getter
public class JoinFunction implements JoinAble {
    @NonNull
    JoinTypeEnum joinTypeEnum;
    JsonNode jsonNode;
    @NonNull
    List<JoinKey> joinKeys;

    JoinFunction(JoinTypeEnum joinTypeEnum, JsonNode jsonNode, List<JoinKey> joinKeys) {
        this.joinTypeEnum = joinTypeEnum;
        this.jsonNode = jsonNode;
        this.joinKeys = joinKeys;
    }

    public static JoinFunctionBuilder builder() {
        return new JoinFunctionBuilder();
    }


    public static class JoinFunctionBuilder {
        private JoinTypeEnum joinTypeEnum;
        private JsonNode jsonNode;
        private List<JoinKey> joinKeys;

        JoinFunctionBuilder() {
        }
        public JoinFunctionBuilder jsonNode(JsonNode jsonNode) {
            this.jsonNode = jsonNode;
            return this;
        }

        private void setDetails(){
            joinKeys = Lists.newLinkedList();
            joinTypeEnum = JoinTypeEnum.valueOf(
                    Optional.ofNullable(jsonNode.get("action")).orElseThrow(()->new IllegalArgumentException("Missing action field"))
                            .asText());
            JsonNode keyNode = Optional.ofNullable(jsonNode.get("keys")).orElseThrow(()->new IllegalArgumentException("Join keys missing"));
            if (!keyNode.isArray()) {
                throw new IllegalArgumentException("keys should be lists");
            }
            JoinAble.JoinKey.JoinKeyBuilder joinKeyBuilder = JoinAble.JoinKey.builder();
            ArrayNode arrayNode = (ArrayNode) keyNode;
            arrayNode.forEach(
                    jCNode->{
                        joinKeyBuilder.leftInx(
                                Optional.ofNullable(jCNode.get("left")).orElseThrow(()->new IllegalArgumentException("left should not null")).asInt()
                        );
                        joinKeyBuilder.rightInx(
                                Optional.ofNullable(jCNode.get("right")).orElseThrow(()->new IllegalArgumentException("right should not null")).asInt()
                        );
                        JsonNode labelNode = Optional.ofNullable(jCNode.get("labels")).orElseThrow(()->new IllegalArgumentException("labels in key missing"));
                        joinKeyBuilder.leftLabel(
                            Optional.ofNullable(labelNode.get("left")).orElseThrow(()->new IllegalArgumentException("left label missing")).asText()
                        );
                        joinKeyBuilder.rightLabel(
                            Optional.ofNullable(labelNode.get("right")).orElseThrow(()->new IllegalArgumentException("right label missing")).asText()
                        );
                        List<JoinAble.KeyColumn> keyColumnList = Lists.newLinkedList();
                        JsonNode jNodeOn = Optional.ofNullable(jCNode.get("on")).orElseThrow(()->new IllegalArgumentException("Missing 'on' in join"));
                        if(!jNodeOn.isArray()){
                            throw new IllegalArgumentException("key join 'ON' should be array");
                        }
                        ArrayNode arrayOnNode = (ArrayNode) jNodeOn;
                        arrayOnNode.forEach(
                                jOnChild->{
                                    JoinAble.KeyColumn.KeyColumnBuilder keyColumnBuilder = JoinAble.KeyColumn.builder();
                                    keyColumnList.add(
                                            keyColumnBuilder
                                                    .leftColumn(Optional.ofNullable(jOnChild.get("left")).orElseThrow(()->new IllegalArgumentException("Missing left in join")).asText())
                                                    .rightColumn(Optional.ofNullable(jOnChild.get("right")).orElseThrow(()->new IllegalArgumentException("Missing right in join")).asText())
                                                    .build()
                                    );
                                }
                        );
                        joinKeyBuilder.columnLst(keyColumnList);
                        joinKeys.add(joinKeyBuilder.build());
                    }
            );

        }

        public JoinFunction build() {
            setDetails();
            return new JoinFunction(joinTypeEnum, jsonNode, joinKeys);
        }

        public String toString() {
            return "JoinFunction.JoinFunctionBuilder(joinTypeEnum=" + this.joinTypeEnum + ", jsonNode=" + this.jsonNode + ", joinKeys=" + this.joinKeys + ")";
        }
    }
}
