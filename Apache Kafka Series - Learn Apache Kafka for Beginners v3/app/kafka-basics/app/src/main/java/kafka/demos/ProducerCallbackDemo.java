package kafka.demos;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallbackDemo {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    public static void run(String[] args) {
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVER"));

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String,String>("foo_topic", "Mensagem " + i + "com callbacks!");

            // Envia uma mensagem junto com uma callback
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // Callback executa sempre que o record é enviado com sucesso ou
                    // caso uma exceção seja disparada
                    if (exception != null) {
                        // Caso a exceção não seja nula então exibe uma mensagem de erro
                        logger.error(exception.getMessage());
                        return;
                    }

                    // Lida com os metadados caso a mensagem tenha sido enviada com
                    // sucesso
                    logger.info("Metadados da mensagem: \n" +
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
