package org.treequery.utils.EventBus.TreeNode;

import com.google.common.collect.Maps;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.SubscriberExceptionContext;
import com.google.common.eventbus.SubscriberExceptionHandler;
import lombok.extern.slf4j.Slf4j;
import org.treequery.exception.FatalSubscriptionException;
import org.treequery.utils.EventBus.EventBusListener;
import org.treequery.utils.EventBus.EventBusSubject;
import org.treequery.dto.TreeNodeEvent;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

@Slf4j
@Deprecated
public class TreeNodeExEventBus implements EventBusSubject<TreeNodeEvent> {
    protected final Executor executor;
    private EventBus localEventBus ;
    private Consumer<Throwable> exceptionCallback;

    private Map<String, EventBusListener> observers = Maps.newConcurrentMap();

    public TreeNodeExEventBus(Executor executor, Consumer<Throwable> exceptionCallback){
        this.executor = executor;
        localEventBus = new AsyncEventBus(executor, this.new MySubscriptionErrorHandler());
        this.exceptionCallback = exceptionCallback;
    }

    @Override
    public void registerObserver(EventBusListener eventBusListener) {
        localEventBus.register(eventBusListener);
        observers.put(eventBusListener.getListenerId(), eventBusListener);
    }

    @Override
    public void removeObserver(String listenerId) {
       if(observers.containsKey(listenerId)) {
           localEventBus.unregister(observers.get(listenerId));
           observers.remove(listenerId);
       }
    }

    @Override
    public void notifyObservers(TreeNodeEvent treeNodeEvent) {
        localEventBus.post(treeNodeEvent);
    }


    public Collection getActiveListeners() {
        return observers.values();
    }

    class MySubscriptionErrorHandler implements SubscriberExceptionHandler{
        @Override
        public void handleException(Throwable exception, SubscriberExceptionContext context) {
            if(exception instanceof FatalSubscriptionException){
                FatalSubscriptionException fatalSubscriptionException = (FatalSubscriptionException) exception;
                log.error(String.format("Fatal error: break the subscription:%s"
                        ,fatalSubscriptionException.getListenerId() ));
                removeObserver(fatalSubscriptionException.getListenerId());
                exceptionCallback.accept(exception);
            }
        }
    }

}
