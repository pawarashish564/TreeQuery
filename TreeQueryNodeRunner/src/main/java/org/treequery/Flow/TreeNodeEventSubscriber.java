package org.treequery.Flow;

import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.treequery.dto.TreeNodeEvent;
import org.treequery.exception.FailClusterRunException;
import org.treequery.service.StatusTreeQueryCluster;
import org.treequery.service.TreeQueryClusterService;
import org.treequery.utils.EventBus.EventBusInterface;

import java.util.concurrent.Flow;

@Slf4j
@Builder
public class TreeNodeEventSubscriber implements Flow.Subscriber<TreeNodeEvent>{
    private Flow.Subscription subscription;
    @NonNull final TreeQueryClusterService treeQueryClusterService;
    @NonNull final EventBusInterface eventBusInterface;

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    @SneakyThrows
    public void onNext(TreeNodeEvent item) {
        subscription.request(1);
        treeQueryClusterService.runQueryTreeNetwork(item.getCalcNode(),
                statusTreeQueryCluster -> {
                    final StatusTreeQueryCluster.QueryTypeEnum Status = statusTreeQueryCluster.getStatus();
                    if (Status == StatusTreeQueryCluster.QueryTypeEnum.FAIL ||
                        Status == StatusTreeQueryCluster.QueryTypeEnum.SYSTEMERROR){
                        throw new FailClusterRunException(statusTreeQueryCluster);
                    }
                    //Push the next waiting node
                    eventBusInterface.put(item.getNotifyNode());
                });
    }

    @Override
    public void onError(Throwable throwable) {
        log.error(throwable.toString());
    }

    @Override
    public void onComplete() {
        log.info("last run completed");
    }
}
