package org.treequery.beam.transform.query;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.KV;

import org.treequery.Transform.function.SqlQueryFunction;

import java.sql.ResultSet;

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
            throw new NoSuchMethodError("Not yet implemented");
        }
    }
}
