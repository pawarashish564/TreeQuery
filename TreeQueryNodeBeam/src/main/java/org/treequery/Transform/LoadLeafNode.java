package org.treequery.Transform;

import org.treequery.model.DataSource;
import org.treequery.model.Node;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class LoadLeafNode extends Node implements DataSource {
    String source;
    String avro_schema;


}
