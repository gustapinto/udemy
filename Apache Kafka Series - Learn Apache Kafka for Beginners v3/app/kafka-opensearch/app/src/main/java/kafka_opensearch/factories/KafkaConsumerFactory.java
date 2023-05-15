package kafka_opensearch.factories;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerFactory {
    public static KafkaConsumer<String, String> make(String kafkaBootstrapServer, String groupId) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBootstrapServer);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        return new KafkaConsumer<>(properties);
    }
}
