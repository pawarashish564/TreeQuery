package org.treequery.grpc.service;

import org.apache.avro.generic.GenericRecord;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.PreprocessInput;
import org.treequery.service.ReturnResult;

import java.util.function.Consumer;

public interface TreeQueryBeamService {
    public PreprocessInput preprocess(String jsonInput);
    public ReturnResult runAndPageResult(TreeQueryRequest.RunMode runMode,
                                         PreprocessInput preprocessInput,
                                         boolean renewCache,
                                         long pageSize,
                                         long page,
                                         Consumer<GenericRecord> dataConsumer);
}
