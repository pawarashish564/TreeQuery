package org.treequery.service;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@Builder
@ToString
public class StatusTreeQueryCluster {
    @RequiredArgsConstructor
    public enum QueryTypeEnum {
        SUCCESS(0), FAIL(2), RUNNING(1);
        @Getter
        private final int value;
    }

    QueryTypeEnum status;
    String description;
}
