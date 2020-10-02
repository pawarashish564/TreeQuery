package org.treequery.utils.EventBus;

@Deprecated
public interface EventBusListener<T> {
    public void listenTreeNodeEvent(T treeNodeEvent);
    public String getListenerId();
}
