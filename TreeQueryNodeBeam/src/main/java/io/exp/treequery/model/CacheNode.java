package io.exp.treequery.model;

import io.exp.treequery.execute.cache.CacheInputInterface;
import io.exp.treequery.model.Node;
import lombok.Builder;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Optional;

//Reference Avro: https://avro.apache.org/docs/current/gettingstartedjava.html#Serializing
@Getter
public  class CacheNode extends Node implements DataSource{
    protected Node originalNode;
    private CacheInputInterface cacheInputInterface;
    public CacheNode(){
        super();
    }

    @Builder
    CacheNode(Node node, CacheInputInterface cacheInputInterface){
        this();
        this.originalNode = node;
        this.cacheInputInterface = cacheInputInterface;
        this.description = node.getDescription();
        this.cluster = node.getCluster();
        this.action = node.getAction();
    }

    public Object getRetrievedValue(){
        return Optional.of(cacheInputInterface).map(
               cacheIO->cacheIO.getRetrievedValue(originalNode.getIdentifier())
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

    @Override
    public String getSource() {
        return this.originalNode.getIdentifier();
    }

    @Override
    public String getAvro_schema() {
        return this.cacheInputInterface.getSchema(this.originalNode.getIdentifier());
    }
}
