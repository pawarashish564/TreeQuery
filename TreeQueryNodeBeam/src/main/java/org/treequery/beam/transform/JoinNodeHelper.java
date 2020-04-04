package org.treequery.beam.transform;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.treequery.Transform.JoinNode;
import org.treequery.Transform.function.JoinFunction;
import org.treequery.Transform.function.NoJoinAbleFunction;
import org.treequery.model.JoinAble;
import org.treequery.model.Node;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.treequery.util.GenericRecordSchemaHelper;

@Slf4j
public class JoinNodeHelper implements NodeBeamHelper{
    @Override
    public PCollection<GenericRecord> apply(Pipeline pipeline, List<PCollection<GenericRecord>> parentCollectionLst, Node node) {
        if (node.getJoinFunction() instanceof NoJoinAbleFunction){
            throw new IllegalArgumentException(String.format("%s is joinable Node", node.toString()));
        }

        JoinFunction joinFunction = (JoinFunction) ((JoinNode)node).getJoinFunction();
        List<JoinAble.JoinKey> joinKeyList = joinFunction.getJoinKeys();

        if (joinKeyList.size() > 1){
            throw new IllegalStateException("Not support join more than 2 stream");
        }

        PCollection<GenericRecord> result = null;
        //Only support Join of 2 stream at this stage
        for (JoinAble.JoinKey joinKey : joinKeyList){

            List<String> leftColumnLst = joinKey.getColumnStream().map(
                    leftColumn->leftColumn.getLeftColumn()
            ).collect(Collectors.toList());
            PCollection< KV<String, GenericRecord> > leftRecord = parentCollectionLst.get(joinKey.getLeftInx()).apply(
                new KeyGenericRecordTransform(leftColumnLst)
            );
            List<String> rightColumnLst = joinKey.getColumnStream().map(
                    rightColumn->rightColumn.getRightColumn()
            ).collect(Collectors.toList());
            PCollection< KV<String, GenericRecord> > rightRecord = parentCollectionLst.get(joinKey.getRightInx()).apply(
                new KeyGenericRecordTransform(rightColumnLst)
            );
            PCollection<KV<String, KV<GenericRecord, GenericRecord>>> joinResult = null;
            switch (joinFunction.getJoinTypeEnum()){
                case INNER_JOIN:
                    joinResult = Join.innerJoin(leftRecord, rightRecord);
                    break;
                default:
                    throw new NoSuchMethodError("Not support non-inner join");
            }

            result = joinResult.apply(ParDo.of(
                    new DoFn<KV<String, KV<GenericRecord, GenericRecord>>, GenericRecord>() {
                        @ProcessElement
                        public void processElement(@Element KV<String, KV<GenericRecord, GenericRecord>> element, OutputReceiver< GenericRecord > out) {
                            log.debug(element.getKey());
                            log.debug(element.getValue().toString());
                        }
                    }
            ));
        }
        return result;
    }


    private static class KeyGenericRecordTransform extends PTransform <PCollection<GenericRecord>, PCollection<KV<String, GenericRecord>> >{
        @NonNull
        List<String> columnLst;
        KeyGenericRecordTransform(List<String> columnLst){
            this.columnLst = columnLst;
        }

        @Override
        public PCollection<KV<String, GenericRecord>> expand(PCollection<GenericRecord> records) {
            PCollection< KV <String, GenericRecord> > keyGenericRecord = records.apply(
                ParDo.of(
                        new DoFn<GenericRecord, KV<String, GenericRecord>>() {
                            @ProcessElement
                            public void processElement(@Element GenericRecord element, OutputReceiver< KV<String, GenericRecord> > out) {
                                String key = getKeyStringHelper(element, columnLst);
                                out.output(KV.of(key, element));
                            }
                        }
                )
            );
            return keyGenericRecord;
        }
    }


    static String getKeyStringHelper (GenericRecord genericRecord, List<String> columnLst){
        StringBuilder sb = new StringBuilder();
        for (String column: columnLst){
            String value = GenericRecordSchemaHelper.StringifyAvroValue(genericRecord, column);
            sb.append(value);
            sb.append("-");
        }
        return sb.toString();
    }


}
