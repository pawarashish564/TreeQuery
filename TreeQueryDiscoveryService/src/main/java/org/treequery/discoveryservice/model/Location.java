package org.treequery.discoveryservice.model;

import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@ToString
public class Location implements Serializable {
    private String address;
    private int port;
}
