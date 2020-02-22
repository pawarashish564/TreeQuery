package io.exp.treequery.model;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;


@Getter
public abstract class Node implements Command, Serializable {
    String description;
    ActionTypeEnum action;
    Command [] childElements;

}
