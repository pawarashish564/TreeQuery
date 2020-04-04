package org.treequery.util;

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
        public Double getValue(){ return value[0]; }
    }

    public static class IntField implements Consumer<Object>{
        Integer[] value = {0};
        @Override
        public void accept(Object o) {
            Integer v = (Integer)o;
            value[0] = v;
        }
        public Integer getValue(){return value[0];}
    }
    public static class EnumSymbolField implements Consumer<Object>{
        GenericData.EnumSymbol[] value = {null};
        @Override
        public void accept(Object obj) {
            GenericData.EnumSymbol e = (GenericData.EnumSymbol) obj;
        }
        public GenericData.EnumSymbol getValue(){return value[0];}
    }

    public static String StringifyAvroValue(GenericRecord genericRecord, String path){
        StringBuilder sb = new StringBuilder();
        Schema.Type schemaType = GenericRecordSchemaHelper.getSchemaType(genericRecord.getSchema(),path);

        switch (schemaType){
            case STRING:
                GenericRecordSchemaHelper.StringField stringField = new GenericRecordSchemaHelper.StringField();
                GenericRecordSchemaHelper.getValue(genericRecord,path, stringField);
                sb.append(stringField.getValue());
                break;
            case INT:
                GenericRecordSchemaHelper.IntField intField = new GenericRecordSchemaHelper.IntField();
                GenericRecordSchemaHelper.getValue(genericRecord, path, intField);
                sb.append(intField.getValue().toString());
                break;
            case DOUBLE:
                GenericRecordSchemaHelper.DoubleField doubleField = new GenericRecordSchemaHelper.DoubleField();
                GenericRecordSchemaHelper.getValue(genericRecord, path, doubleField);
                sb.append(doubleField.getValue().toString());
                break;
            case ENUM:
                GenericRecordSchemaHelper.EnumSymbolField enumSymbolField = new GenericRecordSchemaHelper.EnumSymbolField();
                GenericRecordSchemaHelper.getValue(genericRecord, path, enumSymbolField);
                sb.append(enumSymbolField.getValue().toString());
                break;
            default:
                throw new IllegalArgumentException(String.format("%s can not convert to String", path));
        }
        return sb.toString();
    }

}
