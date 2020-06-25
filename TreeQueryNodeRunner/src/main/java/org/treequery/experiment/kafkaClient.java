package org.treequery.experiment;

import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;

class Employee{

}
public class kafkaClient {
    private static Producer<Long, Employee> createProducer() {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,

                LongSerializer.class.getName());

        // Configure the KafkaAvroSerializer.

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,

                KafkaAvroSerializer.class.getName());

        // Schema Registry location.

        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,

                "http://localhost:8081");

        return new KafkaProducer<>(props);

    }


}

