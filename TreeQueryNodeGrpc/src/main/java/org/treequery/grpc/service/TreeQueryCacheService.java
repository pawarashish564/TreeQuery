package org.treequery.grpc.service;

import org.apache.avro.generic.GenericRecord;
import org.treequery.service.CacheResult;

import java.util.function.Consumer;

public interface TreeQueryCacheService {
    public CacheResult get(String identifier, long pageSize, long page, Consumer<GenericRecord> dataConsumer);
}
