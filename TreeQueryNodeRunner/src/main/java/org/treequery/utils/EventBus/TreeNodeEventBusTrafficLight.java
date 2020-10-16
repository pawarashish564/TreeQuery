package org.treequery.utils.EventBus;

import org.treequery.cluster.ClusterDependencyGraph;
import org.treequery.service.StatusTreeQueryCluster;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface TreeNodeEventBusTrafficLight<T> {
    //after queuing up, return a map of queueId
    Map<T, String> queueUp(String id, List<T> waitCalcNode,
                           Consumer<StatusTreeQueryCluster> triggerEventwhenAllFinish);
    //when unqueue, submit the queueId given by "queueup"
    boolean unqueue(String queueId, StatusTreeQueryCluster statusTreeQueryCluster);
}
