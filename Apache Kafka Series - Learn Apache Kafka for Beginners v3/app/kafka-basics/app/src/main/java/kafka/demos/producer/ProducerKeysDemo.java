package kafka.demos.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerKeysDemo {
    private static final Logger logger = LoggerFactory.getLogger(ProducerKeysDemo.class);

    public static void run(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVER"));
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            String key = "key_" + i;
            String value = "Mensagem " + i + "com callbacks!";

            ProducerRecord<String, String> producerRecord = new ProducerRecord<String,String>("foo_topic", key ,value);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        logger.error(exception.getMessage());
                        return;
                    }

                    logger.info("Chave: " + key + "\n" +
                        "Tópico: " + metadata.topic() + "\n" +
                        "Partição: " + metadata.partition() + "\n" +
                        "Offset: " + metadata.offset() + "\n" +
                        "Timestamp: " + metadata.timestamp() + "\n");
                }
            });
        }

        producer.flush();
        producer.close();
    }
}