package org.treequery.service.proxy;

import org.apache.avro.generic.GenericRecord;
import org.treequery.cluster.Cluster;
//import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.model.Node;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.PreprocessInput;
import org.treequery.service.ReturnResult;
import org.treequery.service.StatusTreeQueryCluster;
import org.treequery.service.TreeQueryClusterRunner;

import java.util.function.Consumer;

public interface TreeQueryClusterRunnerProxyInterface extends TreeQueryClusterRunner {
}
