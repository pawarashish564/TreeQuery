package org.treequery.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.treequery.config.TreeQuerySetting;
import org.treequery.exception.CacheNotFoundException;
import org.treequery.exception.NoException;
import org.treequery.model.CacheTypeEnum;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Consumer;

@Slf4j
public class AvroIOHelper {

    public static void readAvroGenericRecordFile(File avroFile, Schema schema, Consumer<GenericRecord> dataConsumer) throws IOException {
        DatumReader<GenericRecord> datumReader = Optional.ofNullable(schema)
                .map(s -> new GenericDatumReader<GenericRecord>(s))
                .orElse(new GenericDatumReader<GenericRecord>());
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);
        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
            dataConsumer.accept(record);
        }
    }

    public static <T> void readAvroSpecifClassFile(File avroFile, Class c, Consumer<T> dataConsumer) throws IOException {
        DatumReader<T> userDatumReader = new SpecificDatumReader<T>(c);
        DataFileReader<T> dataFileReader = new DataFileReader<T>(avroFile, userDatumReader);
        T record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
            dataConsumer.accept(record);
        }
    }


    private static String getReadFileNameFromIdentifier(TreeQuerySetting treeQuerySetting, String identifier){
        return String.format("%s/%s.avro", treeQuerySetting.getCacheFilePath(), identifier);
    }

    public static void getStreamRecordFromAvroCache(TreeQuerySetting treeQuerySetting,
                                                    String identifier,
                                                    Consumer<GenericRecord> dataConsumer,
                                                    Consumer<Throwable> finishCallback) throws CacheNotFoundException{
        try {
            if (treeQuerySetting.getCacheTypeEnum() == CacheTypeEnum.FILE) {
                String readFileName = getReadFileNameFromIdentifier(treeQuerySetting, identifier);
                AvroIOHelper.getFromAvroFile(readFileName, dataConsumer, finishCallback);
            }
            return;
        }catch(IOException ioe){
            log.error(ioe.getMessage());
            finishCallback.accept( new CacheNotFoundException(String.format("Not able to fetch cache %s from %s",identifier, treeQuerySetting.toString())));
        }catch(CacheNotFoundException ce){
            log.info(ce.getMessage());
            finishCallback.accept( new CacheNotFoundException(String.format("Cache %s not found", identifier)));
        }
        finishCallback.accept( new NoSuchMethodError("Only File Cache implemented"));
    }
    public static Schema getPageRecordFromAvroCache(TreeQuerySetting treeQuerySetting, String identifier, long pageSize, long page, Consumer<GenericRecord> dataConsumer) throws CacheNotFoundException{
        try {
            if (treeQuerySetting.getCacheTypeEnum() == CacheTypeEnum.FILE) {
                String readFileName = getReadFileNameFromIdentifier(treeQuerySetting, identifier);
                return AvroIOHelper.getPageRecordFromAvroFile(readFileName, pageSize, page, dataConsumer);
            }
        }catch(IOException ioe){
            log.error(ioe.getMessage());
            throw new CacheNotFoundException(String.format("Not able to fetch cache %s from %s",identifier, treeQuerySetting.toString()));
        }catch(CacheNotFoundException ce){
            log.info(ce.getMessage());
            throw new CacheNotFoundException(String.format("Cache %s not found", identifier));
        }
        throw new NoSuchMethodError("Only File Cache implemented");
    }

    private static DataFileReader<GenericRecord> getDataFileReader(String avroFileName)throws IOException, CacheNotFoundException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        String readFileName = avroFileName;
        Path filepath = Paths.get(readFileName);
        if (!(Files.exists(filepath) && Files.isReadable(filepath) )){
            throw new CacheNotFoundException(String.format("Cache not found for %s", avroFileName));
        }
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(readFileName), datumReader);
        return dataFileReader;
    }

    //Warning: the output GenericRecord reusing the same instance without duplication from source
    public static void getFromAvroFile(String avroFileName,
                                       Consumer<GenericRecord> dataConsumer,
                                       Consumer<Throwable> finishCallback) throws IOException, CacheNotFoundException {
        DataFileReader<GenericRecord> dataFileReader = getDataFileReader(avroFileName);
        GenericRecord recordPt = null;
        while (dataFileReader.hasNext()) {
            recordPt  = dataFileReader.next(recordPt);
            dataConsumer.accept(recordPt);
        }
        finishCallback.accept(new NoException());
    }

    public static Schema getPageRecordFromAvroFile(String avroFileName, long pageSize, long page, Consumer<GenericRecord> dataConsumer) throws IOException, CacheNotFoundException {
        Schema schema;
        if (page < 1) {
            throw new IllegalArgumentException(String.format("page should be> 1 but received %d", page));
        }
        if (pageSize < 1) {
            throw new IllegalArgumentException(String.format("pageSize should be> 1 but received %d", page));
        }
        DataFileReader<GenericRecord> dataFileReader =  getDataFileReader(avroFileName);
        schema = dataFileReader.getSchema();
        GenericRecord recordPt = null;
        long counter = 0;
        long lessThan = (page - 1) * pageSize;
        long GreaterThan = (page) * pageSize;
        while (dataFileReader.hasNext()) {
            counter++;

            recordPt = dataFileReader.next(recordPt);
            if (counter > lessThan && counter <= GreaterThan) {
                GenericData.Record data = (GenericData.Record) recordPt;
                GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(data);
                dataConsumer.accept(genericRecordBuilder.build());
            }
            if (counter >= GreaterThan) {
                break;
            }
        }
        return schema;
    }

    public static Schema getSchemaFromAvroCache(TreeQuerySetting treeQuerySetting, String identifier) throws CacheNotFoundException{
        try {
            if (treeQuerySetting.getCacheTypeEnum() == CacheTypeEnum.FILE) {
                String readFileName = getReadFileNameFromIdentifier(treeQuerySetting, identifier);
                return AvroIOHelper.getSchemaFromAvroFile(readFileName);
            }
        }catch(IOException ioe){
            log.error(ioe.getMessage());
            throw new CacheNotFoundException(String.format("Not able to fetch cache %s from %s",identifier, treeQuerySetting.toString()));
        }
        throw new NoSuchMethodError("Only File Cache implemented");
    }

    public static Schema getSchemaFromAvroFile(String avroFileName) throws IOException{
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        String readFileName = avroFileName;
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(readFileName), datumReader);
        return dataFileReader.getSchema();
    }
}
