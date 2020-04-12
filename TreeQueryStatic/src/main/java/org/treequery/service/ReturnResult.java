package org.treequery.service;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.avro.Schema;

import java.io.Serializable;

@Builder
@Getter
public  class ReturnResult implements Serializable {
    @NonNull
    String hashCode;
    @NonNull
    StatusTreeQueryCluster statusTreeQueryCluster;
    Schema dataSchema;
}
