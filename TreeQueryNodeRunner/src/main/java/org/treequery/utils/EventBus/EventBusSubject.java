package org.treequery.utils.EventBus;

import java.util.Collection;
import java.util.List;

@Deprecated
public interface EventBusSubject<T> {
    public void registerObserver(EventBusListener<T> eventBusListener);
    public void removeObserver(String listenerId);

    public void notifyObservers(T treeNodeEvent);
    public Collection getActiveListeners();
}
