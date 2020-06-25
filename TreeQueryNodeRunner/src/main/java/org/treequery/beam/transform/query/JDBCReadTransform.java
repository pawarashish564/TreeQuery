package org.treequery.beam.transform.query;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.treequery.Transform.function.SqlQueryFunction;
import org.treequery.exception.JDBCConversionRuntimeException;

import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class JDBCReadTransform {

    public static JdbcIO.Read<GenericRecord> getJDBCRead(SqlQueryFunction sqlQueryFunction, Schema schema){
        AvroCoder avroCoder = AvroCoder.of(GenericRecord.class, schema);

        return JdbcIO.< GenericRecord >read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                        sqlQueryFunction.getDriverClassName(),
                        sqlQueryFunction.getSqlConnString())
                        .withUsername(sqlQueryFunction.getUsername())
                        .withPassword(sqlQueryFunction.getPassword()))
                .withQuery(sqlQueryFunction.getQuery())
                .withCoder(avroCoder)
                .withRowMapper(
                        JDBCResult2GenericRecordMapper.builder()
                        .schema(schema)
                        .build()
                );

    }

    @Builder
    static class JDBCResult2GenericRecordMapper implements JdbcIO.RowMapper< GenericRecord >{
        private final Schema schema;
        @Override
        public GenericRecord mapRow(ResultSet resultSet) throws Exception {
            log.debug(resultSet.toString());
            GenericRecord record = new GenericData.Record(schema);
            schema.getFields().forEach(
                    field -> {
                        Schema.Type type = field.schema().getType();
                        setGenericRecordValueHelper(record, resultSet, field);
                    }
            );

            return record;
        }
    }

    static GenericRecord setGenericRecordValueHelper(GenericRecord genericRecord, ResultSet resultSet, Schema.Field field)  {
        final String fieldName = field.name();
        final Schema.Type type = field.schema().getType();
        try {
            switch (type) {
                case STRING:
                    genericRecord.put(fieldName, resultSet.getString(fieldName));
                    break;
                case INT:
                    genericRecord.put(fieldName, resultSet.getInt(fieldName));
                    break;
                case DOUBLE:
                    genericRecord.put(fieldName, resultSet.getDouble(fieldName));
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Invalid value type from SQL query:%s with type %s", fieldName, type.getName()));
            }
        }catch(SQLException sqe){
            sqe.printStackTrace();
            throw new JDBCConversionRuntimeException(sqe.getMessage());
        }

        return genericRecord;
    }


}
