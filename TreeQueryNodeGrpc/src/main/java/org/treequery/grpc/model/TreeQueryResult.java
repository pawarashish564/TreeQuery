package org.treequery.grpc.model;

import lombok.Builder;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.treequery.grpc.client.TreeQueryClient;

import java.util.List;

@Getter
@Builder
public class TreeQueryResult {
    String requestHash;
    TreeQueryResponseHeader header;
    TreeQueryResponseResult result;

    @Builder
    @Getter
    public static class TreeQueryResponseHeader{
        boolean success;
        int err_code;
        String err_msg;
    }
    @Builder
    @Getter
    public static class TreeQueryResponseResult{
        long pageSize;
        long page ;
        long datasize;
        List<GenericRecord> genericRecordList;
        Schema schema;
    }
}
