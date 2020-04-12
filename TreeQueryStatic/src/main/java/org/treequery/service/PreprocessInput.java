package org.treequery.service;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.treequery.model.Node;

import java.io.Serializable;

@Builder
@Getter
public class PreprocessInput implements Serializable {
    @NonNull
    private final Node node;
    @NonNull
    private final Schema outputSchema;
}