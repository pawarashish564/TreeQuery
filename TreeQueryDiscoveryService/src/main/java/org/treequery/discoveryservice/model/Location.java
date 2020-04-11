package org.treequery.discoveryservice.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;

@RequiredArgsConstructor
@Getter
public class Location implements Serializable {
    private final String address;
    private final int port;
}
