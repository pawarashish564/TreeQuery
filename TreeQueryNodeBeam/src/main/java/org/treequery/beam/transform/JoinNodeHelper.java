package org.treequery.beam.transform;

import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.treequery.Transform.JoinNode;
import org.treequery.Transform.function.JoinFunction;
import org.treequery.Transform.function.NoJoinAbleFunction;
import org.treequery.exception.JoinOnlySupport2NodesException;
import org.treequery.utils.AvroSchemaHelper;
import org.treequery.model.JoinAble;
import org.treequery.model.Node;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.treequery.utils.GenericRecordSchemaHelper;

@Slf4j
@RequiredArgsConstructor
public class JoinNodeHelper implements NodeBeamHelper{
    protected final AvroSchemaHelper avroSchemaHelper;
    @Override
    public PCollection<GenericRecord> apply(Pipeline pipeline, List<PCollection<GenericRecord>> parentCollectionLst, Node node) {
        if (node.getJoinFunction() instanceof NoJoinAbleFunction){
            throw new IllegalArgumentException(String.format("%s is joinable Node", node.toString()));
        }

        JoinFunction joinFunction = (JoinFunction) ((JoinNode)node).getJoinFunction();
        List<JoinAble.JoinKey> joinKeyList = joinFunction.getJoinKeys();

        if (joinKeyList.size() > 1){
            throw new JoinOnlySupport2NodesException(node);
        }

        PCollection<GenericRecord> result = null;
        //Only support Join of 2 stream at this stage
        for (JoinAble.JoinKey joinKey : joinKeyList){

            List<String> leftColumnLst = joinKey.getColumnStream().map(
                    leftColumn->leftColumn.getLeftColumn()
            ).collect(Collectors.toList());
            Schema leftSchema = avroSchemaHelper.getAvroSchema(node.getChildren().get(joinKey.getLeftInx()));
            AvroCoder leftCoder = AvroCoder.of(GenericRecord.class, leftSchema);
            PCollection<GenericRecord> leftCollection = parentCollectionLst.get(joinKey.getLeftInx());
            leftCollection.setCoder(leftCoder);
            PCollection< KV<String, GenericRecord> > leftRecord = leftCollection.apply(
                 new KeyGenericRecordTransform(leftColumnLst)
            );
            List<String> rightColumnLst = joinKey.getColumnStream().map(
                    rightColumn->rightColumn.getRightColumn()
            ).collect(Collectors.toList());

            Schema rightSchema = avroSchemaHelper.getAvroSchema(node.getChildren().get(joinKey.getRightInx()));
            AvroCoder rightCoder = AvroCoder.of(GenericRecord.class, rightSchema);
            PCollection<GenericRecord> rightCollection = parentCollectionLst.get(joinKey.getRightInx());
            rightCollection.setCoder(rightCoder);
            PCollection< KV<String, GenericRecord> > rightRecord = rightCollection.apply(
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

            Schema outputSchema = avroSchemaHelper.getAvroSchema(node);
            AvroCoder outputCoder = AvroCoder.of(GenericRecord.class, outputSchema);
            result = joinResult.apply(
                    OutputGenericRecordTransform.builder()
                        .schema(outputSchema)
                        .leftLabel(joinKey.getLeftLabel())
                        .rightLabel(joinKey.getRightLabel())
                        .build()
            ).setCoder(outputCoder);
        }
        return result;
    }
    @Builder
    private static class OutputGenericRecordTransform extends PTransform<PCollection< KV<String, KV<GenericRecord, GenericRecord>> >, PCollection<GenericRecord> >{
        protected final Schema schema;
        protected final String leftLabel;
        protected final String rightLabel;
        @Override
        public PCollection<GenericRecord> expand(PCollection<  KV<String, KV<GenericRecord, GenericRecord>>   > input) {
            PCollection<GenericRecord> output = input.apply(
                    ParDo.of(
                            new DoFn<KV<String, KV<GenericRecord, GenericRecord>>, GenericRecord>() {
                                @ProcessElement
                                public void processElement(@Element KV<String, KV<GenericRecord, GenericRecord>> element, OutputReceiver< GenericRecord > out) {
                                    KV<GenericRecord, GenericRecord> __value = element.getValue();
                                    GenericRecord leftValue = Optional.ofNullable(__value)
                                            .orElseThrow(()->new IllegalArgumentException("failed to get left value"))
                                            .getKey();
                                    GenericRecord rightValue = Optional.ofNullable(__value)
                                            .orElseThrow(()->new IllegalArgumentException("failed to get right value"))
                                            .getValue();
                                    GenericRecord joinResult = new GenericData.Record(schema);
                                    joinResult.put(leftLabel, leftValue);
                                    joinResult.put(rightLabel,rightValue);
                                    out.output(joinResult);
                                }
                            }
                    )
            );
            return output;
        }
    }
    @RequiredArgsConstructor
    private static class KeyGenericRecordTransform extends PTransform <PCollection<GenericRecord>, PCollection<KV<String, GenericRecord>> >{
        @NonNull
        protected final List<String> columnLst;

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
