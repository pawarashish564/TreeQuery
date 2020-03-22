package org.treequery.model;

import org.apache.avro.Schema;

public interface DataSource {
     public String getSource() ;
     public String getAvro_schema() ;

     public default Schema getAvroSchemaObj(){
          Schema.Parser parser = new Schema.Parser();
          Schema schema = parser.parse(this.getAvro_schema());
          return schema;
     }
}
