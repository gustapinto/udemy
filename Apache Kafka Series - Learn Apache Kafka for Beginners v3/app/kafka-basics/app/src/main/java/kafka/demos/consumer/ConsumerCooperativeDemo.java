package kafka.demos.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerCooperativeDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerCooperativeDemo.class);

    public static void run(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVER"));
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "java-app");
        properties.setProperty("auto.offset.reset", "earliest");

        // Configrua a forma como as partições são atribuidas para um consumidor
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList("foo_topic"));

        while (true) {
            logger.info("Polling...");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Chave: " + record.key() + "\n" +
                        "Tópico: " + record.topic() + "\n" +
                        "Partição: " + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n" +
                        "Timestamp: " + record.timestamp() + "\n" +
                        "Valor: " + record.value() + "\n");
            }
        }
    }
}
