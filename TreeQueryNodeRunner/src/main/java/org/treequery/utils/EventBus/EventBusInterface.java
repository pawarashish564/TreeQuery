package org.treequery.utils.EventBus;

public interface EventBusInterface<T> {
    void put(T value) throws InterruptedException;
    T take() throws InterruptedException;
}
