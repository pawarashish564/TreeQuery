package org.treequery.service;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class StatusTreeQueryCluster {
    public enum QueryTypeEnum {
        SUCCESS, FAIL, RUNNING
    }

    QueryTypeEnum status;
    String description;
}
