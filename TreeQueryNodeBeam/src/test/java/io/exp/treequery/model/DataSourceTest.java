package io.exp.treequery.model;

import io.exp.treequery.Transform.LoadLeafNode;
import io.exp.treequery.Transform.TransformNodeFactory;
import io.exp.treequery.beam.FileCacheInputImpl;
import io.exp.treequery.cluster.ClusterDependencyGraph;
import io.exp.treequery.cluster.NodeFactory;
import io.exp.treequery.cluster.NodeTreeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
@Slf4j
class DataSourceTest {

    String workDirectory = null;
    NodeFactory nodeFactory;
    NodeTreeFactory nodeTreeFactory;

    Node rootNode;

    @BeforeEach
    void init() throws Exception{
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        String simpleAvroTree = "SimpleAvroReadCluster.json";

        File jsonFile = new File(classLoader.getResource(simpleAvroTree).getFile());
        workDirectory = jsonFile.getParent();
        String jsonString = this.parseJsonFile(jsonFile.getAbsolutePath());
        jsonString = jsonString.replaceAll("\\$\\{WORKDIR\\}", workDirectory);

        nodeFactory = new TransformNodeFactory();
        nodeTreeFactory = NodeTreeFactory.builder().nodeFactory(nodeFactory).build();
        rootNode = nodeTreeFactory.parseJsonString(jsonString);

    }


    @Test
    void experimentSchemaGetType() throws IOException {
        assertThat(rootNode).isInstanceOf(LoadLeafNode.class);
        LoadLeafNode loadLeafNode = (LoadLeafNode) rootNode;
        Schema schema = loadLeafNode.getAvroSchemaObj();
        assertAll(
                ()->{
            assertEquals(Schema.Type.STRING, GenericRecordSchemaHelper.getSchemaType(schema,"id"));
            assertEquals(Schema.Type.DOUBLE, GenericRecordSchemaHelper.getSchemaType(schema,"asset.notional"));
            assertEquals(Schema.Type.STRING, GenericRecordSchemaHelper.getSchemaType(schema,"asset.securityId"));
            }
        );

        Schema.Type type = GenericRecordSchemaHelper.getSchemaType(schema, "asset.bidask");
        assertThrows(NullPointerException.class, ()->{GenericRecordSchemaHelper.getSchemaType(schema,"asset2.notional");});

        File avroFile = new File(loadLeafNode.getSource());
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);
        GenericRecord trade = null;
        while (dataFileReader.hasNext()) {
            trade = dataFileReader.next(trade);
            StringBuffer id =new StringBuffer();
            GenericRecordSchemaHelper.getValue(trade,"id", (obj)->{
                Utf8 s = (Utf8)obj;
                id.append( s.toString());
            });
            assertThat(id.toString()).isNotBlank();

            StringBuffer secid  = new StringBuffer();
            GenericRecordSchemaHelper.getValue(trade,"asset.securityId", (obj)->{
                Utf8 s = (Utf8)obj;
                secid.append(s.toString());
            });
            assertThat(secid.toString()).isNotBlank();

            Double[] ntl = {0.0};
            GenericRecordSchemaHelper.getValue(trade,"asset.notional", (obj)->{
                Double v = (Double)obj;
                ntl[0] = v;
            });
            assertThat(ntl[0]).isNotNaN();
        }


    }



    private  String parseJsonFile (String jsonFile)  {
        String jsonString="";
        StringBuilder contentBuilder = new StringBuilder();
        try(Stream<String> stream = Files.lines( Paths.get(jsonFile), StandardCharsets.UTF_8)){
            stream.forEach(s -> contentBuilder.append(s));
            jsonString= contentBuilder.toString();
        }catch(IOException ioe){
            log.error(ioe.getMessage());
        }
        return jsonString;
    }

}