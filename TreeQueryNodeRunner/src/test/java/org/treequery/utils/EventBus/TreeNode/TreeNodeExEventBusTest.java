package org.treequery.utils.EventBus.TreeNode;


import com.google.common.eventbus.Subscribe;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.treequery.Transform.JoinNode;
import org.treequery.dto.TreeNodeEvent;
import org.treequery.exception.FatalSubscriptionException;
import org.treequery.model.Node;
import org.treequery.utils.EventBus.EventBusListener;
import org.treequery.utils.EventBus.EventBusSubject;
import org.treequery.utils.JsonInstructionHelper;
import org.treequery.utils.TestDataAgent;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@Slf4j
class TreeNodeExEventBusTest {
    private static final int MAX_THREAD_RUN = Runtime.getRuntime().availableProcessors()-1;
    final static ExecutorService executorService = Executors.newFixedThreadPool(MAX_THREAD_RUN);
    Node node;

    EventBusSubject<TreeNodeEvent> eventEventBusSubject;
    private MockEventBusListener eventEventBusListener;
    private List< TreeNodeEvent > refTreeNodeEventList;
    private static final int numberOfListeners = MAX_THREAD_RUN;
    private CountDownLatch countDownLatch;
    @BeforeEach
    void initTreeNode() throws Exception{
        String AvroTree = "TreeQueryInput4.json";
        String jsonString = TestDataAgent.prepareNodeFromJsonInstruction(AvroTree);
        node = JsonInstructionHelper.createNode(jsonString);
        assertThat(node).isInstanceOf(JoinNode.class);

        refTreeNodeEventList = Lists.newArrayList();
        refTreeNodeEventList.add(TreeNodeEvent.builder()
                .calcNode(node)
                .id(UUID.randomUUID().toString())
                .build());

    }

    void insertMockListener(Consumer<Throwable> exceptionHandler){
        eventEventBusSubject = new TreeNodeExEventBus(executorService, exceptionHandler);
        eventEventBusListener = this.new MockEventBusListener();
        eventEventBusSubject.registerObserver(eventEventBusListener);
    }

    @Test
    @SneakyThrows
    void shouldBroadCasttoOneListerner(){
        countDownLatch = new CountDownLatch(this.refTreeNodeEventList.size());
        insertMockListener(throwable -> {
           log.error(throwable.getMessage());
        });
        refTreeNodeEventList.forEach(
                treeNodeEvent -> {
                    eventEventBusSubject.notifyObservers(treeNodeEvent);
                }
        );

        countDownLatch.await();
        //verify the result
       assertThat(eventEventBusListener.getMyProcessedList()).hasSize(refTreeNodeEventList.size());
       for (int i=0;i<refTreeNodeEventList.size();i++){
           assertEquals(refTreeNodeEventList.get(i),
                   eventEventBusListener.getMyProcessedList().get(i));
       }
    }

    @Test
    @SneakyThrows
    void shouldRemainOneListener() {
        countDownLatch = new CountDownLatch(this.refTreeNodeEventList.size() + 1);
        insertMockListener(throwable -> {
            log.error(throwable.getMessage());
            countDownLatch.countDown();
        });

        MockCriticalFailureEventBusListener mockCriticalFailureEventBusListener = this.new MockCriticalFailureEventBusListener();
        eventEventBusSubject.registerObserver(mockCriticalFailureEventBusListener);

        refTreeNodeEventList.forEach(
                treeNodeEvent -> {
                    eventEventBusSubject.notifyObservers(treeNodeEvent);
                }
        );
        countDownLatch.await();
        Collection checkListeners = eventEventBusSubject.getActiveListeners();
        assertThat(checkListeners).hasSize(1);
    }

    class  MockEventBusListener implements  EventBusListener<TreeNodeEvent>{
        @Getter
        private String listenerId = UUID.randomUUID().toString();
        @Getter
        private List<TreeNodeEvent> myProcessedList = Lists.newArrayList();
        @Override
        @Subscribe
        public void listenTreeNodeEvent(TreeNodeEvent treeNodeEvent) {
            myProcessedList.add(treeNodeEvent);
            countDownLatch.countDown();
        }

    }
    class  MockCriticalFailureEventBusListener implements  EventBusListener<TreeNodeEvent>{
        @Getter
        private String listenerId = UUID.randomUUID().toString();

        @Override
        @Subscribe
        public void listenTreeNodeEvent(TreeNodeEvent treeNodeEvent) {
                throw new FatalSubscriptionException(listenerId, "testing");
        }

    }
}