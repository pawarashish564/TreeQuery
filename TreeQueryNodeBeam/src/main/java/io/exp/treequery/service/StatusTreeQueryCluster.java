package io.exp.treequery.service;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class StatusTreeQueryCluster {
    public enum QueryTypeEnum {
        SUCCESS, FAIL, RUNNING
    }

    QueryTypeEnum status;
    String description;
}
