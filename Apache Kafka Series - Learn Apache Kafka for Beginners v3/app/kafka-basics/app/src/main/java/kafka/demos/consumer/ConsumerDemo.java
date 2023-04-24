package kafka.demos.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void run(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVER"));

        // Propriedades específicas do consumer, fazendo com que o consumer espere
        // por Strings para as chaves e valores
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        // Configura o id do grupo
        properties.setProperty("group.id", "java-app");

        // Configura como os offsets são lidos
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Se inscreve nos tópicos
        consumer.subscribe(Arrays.asList("foo_topic"));

        // Faz o poll por novas mensagens no tópico
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
