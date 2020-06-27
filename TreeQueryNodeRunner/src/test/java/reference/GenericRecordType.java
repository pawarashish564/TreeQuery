package reference;



import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;


// Just for demonistration; not robust implementation
public class GenericRecordType {
    @Test
    public void testName() throws Exception {
        Schema schema = buildSchema();

        GenericRecord record = new Record(schema);
        record.put("clientId", 12);
        record.put("deviceName", "GlassScanner");
        record.put("holder", new HashMap<>());

        Integer value = IntField.clientId.getValue(record);
        String deviceName = StringField.deviceName.getValue(record);
        Map<String, String> mapString = MapOfStringField.holder.getValue(record);

        assertThat(deviceName).isEqualTo("GlassScanner");
        assertEquals(12, value.intValue());
        assertThat(mapString).hasSize(0);
    }

    private Schema buildSchema() {
        Field clientId = new Field("clientId", Schema.create(Type.INT), "hello", (Object) null);
        Field deviceName = new Field("deviceName", Schema.create(Type.STRING), "hello", (Object) null);
        Field holder = new Field("holder", Schema.createMap(Schema.create(Type.STRING)), null, (Object) null);
        Schema schema = Schema.createRecord(Arrays.asList(clientId, deviceName, holder));
        return schema;
    }

    public static interface TypedField<T> {
        String name();

        public T getValue(GenericRecord record);

    }

    public static enum StringField implements TypedField<String> {
        deviceName;

        @Override
        public String getValue(GenericRecord record) {
            String typed = null;
            Object raw = record.get(name());
            if (raw != null) {
                if (!(raw instanceof String || raw instanceof Utf8)) {
                    throw new AvroTypeException("string type was epected for field:" + name());
                }
                typed = raw.toString();
            }
            return typed;
        }

    }

    public static enum IntField implements TypedField<Integer> {
        clientId;

        private IntField() {
        }

        @Override
        public Integer getValue(GenericRecord record) {
            Integer typed = null;
            Object raw = record.get(name());
            if (raw != null) {
                if (!(raw instanceof Integer)) {
                    throw new AvroTypeException("int type was epected for field:" + name());
                }
                typed = (Integer) raw;
            }
            return typed;
        }

    }

    public static enum MapOfStringField implements TypedField<Map<String, String>> {
        holder;

        @Override
        @SuppressWarnings("unchecked")
        public Map<String, String> getValue(GenericRecord record) {
            Map<String, String> typed = null;
            Object raw = record.get(name());
            if (raw != null) {
                if (!(raw instanceof Map)) {
                    throw new AvroTypeException("map type was epected for field:" + name());
                }
                typed = (Map<String, String>) raw;
            }
            return typed;
        }
    }

}