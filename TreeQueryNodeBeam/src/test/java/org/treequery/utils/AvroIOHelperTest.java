package org.treequery.utils;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.treequery.exception.CacheNotFoundException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class AvroIOHelperTest {

    @Test
    void failureGetCacheFile() throws Exception{
        String avroFileName = "/tmp/NotFoundFile.avro";

        assertThrows(CacheNotFoundException.class, ()->{
            AvroIOHelper.getPageRecordFromAvroFile(avroFileName, 1, 5, (record)->{});
        });

    }
}