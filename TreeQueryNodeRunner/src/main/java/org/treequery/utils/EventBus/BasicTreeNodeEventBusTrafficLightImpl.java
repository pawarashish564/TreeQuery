package org.treequery.utils.EventBus;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.treequery.model.Node;
import org.treequery.service.StatusTreeQueryCluster;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Slf4j
public class BasicTreeNodeEventBusTrafficLightImpl implements TreeNodeEventBusTrafficLight<Node>{

    volatile Map<String, Map<String, Boolean> > waitMap = Maps.newHashMap();
    volatile Map<String, Consumer<StatusTreeQueryCluster> > actionMap = Maps.newHashMap();
    Object writeLockObj = new Object();
    final static String keyDelimiter = ":";

    @Override
    public Map<Node, String> queueUp(String id, List<Node> waitCalcNode,
                                     Consumer<StatusTreeQueryCluster> triggerEventwhenAllFinish) {
        Map<Node, String> retResult = Maps.newHashMap();
        synchronized (writeLockObj){
            Map<String, Boolean> childMap = Maps.newLinkedHashMap();
            waitCalcNode.forEach(
                    node -> {
                        String childKey = UUID.randomUUID().toString();
                        childMap.put(childKey, Boolean.FALSE);
                        String queueId = id+keyDelimiter+childKey;
                        retResult.put(node, queueId);
                    }
            );
            waitMap.put(id, childMap);
            actionMap.put(id, triggerEventwhenAllFinish);
        }
        return retResult;
    }

    @Override
    public boolean unqueue(String queueId, StatusTreeQueryCluster statusTreeQueryCluster) {
        String[] keys = queueId.split(keyDelimiter);
        boolean takeAction = true;

        if(waitMap.containsKey(keys[0])){
            Map<String, Boolean> childMap = waitMap.get(keys[0]);
            synchronized (childMap) {
                if (childMap.containsKey(keys[1])) {
                    childMap.put(keys[1], Boolean.TRUE);
                }
                for ( String key : childMap.keySet()){
                    if (!childMap.get(key) ){
                        takeAction = false;
                        break;
                    }
                }
                if (takeAction){
                    Optional.ofNullable(actionMap.get(keys[0])).ifPresent(
                            statusTreeQueryClusterConsumer -> {
                                statusTreeQueryClusterConsumer.accept(statusTreeQueryCluster);
                            }
                    );
                }
            }
        }
        return takeAction;
    }
}
