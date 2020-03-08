package io.exp.treequery.Transform;

import io.exp.treequery.model.DataSource;
import io.exp.treequery.model.Node;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class LoadLeafNode extends Node implements DataSource {
    String source;
    String avro_schema;


}
