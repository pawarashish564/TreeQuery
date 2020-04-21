package org.treequery.grpc.client;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.treequery.grpc.exception.FailConnectionException;
import org.treequery.grpc.model.TreeQueryResult;
import org.treequery.proto.TreeQueryRequest;

import org.treequery.proto.TreeQueryServiceGrpc;


import java.util.List;

@Slf4j
public class TreeQueryClient {
    private final TreeQueryServiceGrpc.TreeQueryServiceBlockingStub blockingStub;

    private GrpcClientChannel grpcClientChannel;

    public TreeQueryClient(String host, int port) {
        grpcClientChannel = new GrpcClientChannel(host, port);
        this.blockingStub = TreeQueryServiceGrpc.newBlockingStub(grpcClientChannel.getChannel());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                log.info("*** shutting down gRPC client since JVM is shutting down");
                grpcClientChannel.shutdown();
                log.info("*** client shut down");
            }
        });
    }

    public TreeQueryResult query(TreeQueryRequest.RunMode runMode,
                                 String jsonInput,
                                 boolean renewCache,
                                 long pageSize,
                                 long page){
        TreeQueryResult treeQueryResult = null;

        TreeQueryRequest.Builder treeQueryRequestBuilder = TreeQueryRequest.newBuilder();
        treeQueryRequestBuilder.setRunMode(runMode);
        treeQueryRequestBuilder.setJsonInput(jsonInput);
        treeQueryRequestBuilder.setRenewCache(renewCache);
        treeQueryRequestBuilder.setPageSize(pageSize);
        treeQueryRequestBuilder.setPage(page);

        org.treequery.proto.TreeQueryResponse  treeQueryResponse = null;

        try {
            TreeQueryResult.TreeQueryResultBuilder treeQueryResultBuilder = TreeQueryResult.builder();
            treeQueryResponse = blockingStub.query(treeQueryRequestBuilder.build());
            treeQueryResultBuilder.requestHash(treeQueryResponse.getRequestHash());

            TreeQueryResult.TreeQueryResponseHeader.TreeQueryResponseHeaderBuilder treeQueryResponseHeaderBuilder =
                    TreeQueryResult.TreeQueryResponseHeader.builder();
            boolean success = treeQueryResponse.getHeader().getSuccess();
            treeQueryResponseHeaderBuilder.success(success);
            treeQueryResponseHeaderBuilder.err_code(treeQueryResponse.getHeader().getErrCode());
            treeQueryResponseHeaderBuilder.err_msg(treeQueryResponse.getHeader().getErrMsg());
            treeQueryResultBuilder.header(treeQueryResponseHeaderBuilder.build());

            TreeQueryResult.TreeQueryResponseResult.TreeQueryResponseResultBuilder treeQueryResponseResultBuilder =
                    TreeQueryResult.TreeQueryResponseResult.builder();
            treeQueryResponseResultBuilder.datasize(treeQueryResponse.getResult().getDatasize());
            treeQueryResponseResultBuilder.page(treeQueryResponse.getResult().getPage());
            treeQueryResponseResultBuilder.pageSize(treeQueryResponse.getResult().getPageSize());


            if (success){
                List<GenericRecord> genericRecordList = Lists.newLinkedList();
                ByteString dataLoadString = treeQueryResponse.getResult().getAvroLoad();
                byte[] dataLoad = dataLoadString.toByteArray();
                String schemaJsonStr = treeQueryResponse.getResult().getAvroSchema();
                Schema.Parser parser = new Schema.Parser();
                Schema schema =  parser.parse(schemaJsonStr);
                DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(dataLoad, null);
                while (!decoder.isEnd()){
                    GenericRecord genericRecord = datumReader.read(null, decoder);
                    genericRecordList.add(genericRecord);
                }
                treeQueryResponseResultBuilder.genericRecordList(genericRecordList);
                treeQueryResponseResultBuilder.schema(schema);
            }
            treeQueryResultBuilder.result(treeQueryResponseResultBuilder.build());
            treeQueryResult = treeQueryResultBuilder.build();

        }catch(StatusRuntimeException se){
            log.error("unable to connect:"+se.getMessage());
            throw new FailConnectionException("unable to connect:"+se.getMessage());
        } catch(Exception ex){
            log.warn("failed to do query:"+ex.getMessage());
            throw new IllegalStateException("failed to do query:"+ex.getMessage());
        }

        return treeQueryResult;
    }
}
