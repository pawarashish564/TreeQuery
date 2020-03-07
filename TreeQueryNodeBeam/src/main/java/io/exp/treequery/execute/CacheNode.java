package io.exp.treequery.execute;

import com.google.gson.Gson;
import io.exp.treequery.model.Node;
import lombok.Getter;
import org.apache.avro.generic.GenericRecord;

//Reference Avro: https://avro.apache.org/docs/current/gettingstartedjava.html#Serializing
@Getter
public abstract class CacheNode extends Node {
    protected Node originalNode;
    protected GenericRecord value;
    public CacheNode(){
        super();
    }
    public CacheNode(Node node){
        this();
        this.originalNode = node;
    }

    public abstract void getRetrievedValue();

    @Override
    public int hashCode() {
        return originalNode.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return originalNode.equals(obj);
    }

    public String toString() {
        Gson gson  = new Gson();
        return gson.toJson(value);
    }
}
