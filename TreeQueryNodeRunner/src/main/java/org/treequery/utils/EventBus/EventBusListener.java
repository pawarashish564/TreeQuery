package org.treequery.utils.EventBus;

public interface EventBusListener<T> {
    public void listenTreeNodeEvent(T treeNodeEvent);
    public String getListenerId();
}
