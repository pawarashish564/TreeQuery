package io.exp.treequery.execute;

import com.google.gson.Gson;
import io.exp.treequery.model.Node;
import lombok.Builder;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Optional;

//Reference Avro: https://avro.apache.org/docs/current/gettingstartedjava.html#Serializing
@Getter
public  class CacheNode extends Node {
    protected Node originalNode;
    protected GenericRecord value;
    protected Schema schema;
    private CacheIOInterface cacheIOInterface;
    public CacheNode(){
        super();
    }

    @Builder
    CacheNode(Node node, CacheIOInterface cacheIOInterface){
        this();
        this.originalNode = node;
        this.cacheIOInterface = cacheIOInterface;
    }

    public void getRetrievedValue(){
        Optional.of(cacheIOInterface).ifPresent(
                cachIO -> {
                    cachIO.getRetrievedValue(originalNode.getIdentifier());
                }
        );
    }

    @Override
    public int hashCode() {
        return originalNode.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return originalNode.equals(obj);
    }

    public String toString() {
        return String.format("Cache(%s)",this.originalNode.toString());
    }
}
