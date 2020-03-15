package io.exp.treequery.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public class GenericRecordSchemaHelper {

    public static Schema.Type getSchemaType(Schema schema, String strPath){
        String [] path = strPath.split("\\.");
        Schema tmpSchema = schema;
        Schema.Field field = null;
        for (String s : path){
            field = Optional.of(tmpSchema.getField(s)).orElseThrow(()->new IllegalArgumentException("Missing field:"+s));
            tmpSchema = field.schema();
        }
        return Optional.of(field).map(f->f.schema().getType()).get();
    }

    public static void getValue(GenericRecord genericRecord, String strPath, Consumer convertFunc){
        String [] path = strPath.split("\\.");
        Object tmpGenericRecord = genericRecord;
        for (String s: path){
            if (tmpGenericRecord instanceof GenericRecord){
                tmpGenericRecord = Optional.of(((GenericRecord)tmpGenericRecord).get(s)).get();
            }else if (tmpGenericRecord instanceof GenericData.Record){
                tmpGenericRecord = Optional.of(((GenericData.Record)tmpGenericRecord).get(s)).get();
            }else{
                throw new IllegalArgumentException(String.format("Not found %s", strPath));
            }

        }
        convertFunc.accept(tmpGenericRecord);

    }

    public static class StringField implements Consumer<Object>{
        StringBuffer value  = new StringBuffer();
        @Override
        public void accept(Object obj) {
            Utf8 s = (Utf8)obj;
            value.append(s.toString());
        }
        public String getValue(){
            return value.toString();
        }
    }

    public static class DoubleField implements Consumer<Object>{
        Double[] value = {0.0};
        @Override
        public void accept(Object obj) {
            Double v = (Double)obj;
            value[0] = v;
        }
        public Double getValue(){
            return value[0];
        }
    }

}
