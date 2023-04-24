package kafka.demos.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerShutdownDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerShutdownDemo.class);

    public static void run(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVER"));
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "java-app");
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Obtém uma referença a thread sendo executada
        final Thread mainThread = Thread.currentThread();

        // Adiciona uma callback de shutdown
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Exiting....");

                // Método usado para parar um poll a partir de uma exceção na próxima
                // vez que o poll executar
                consumer.wakeup();

                // Retornar para a thread principal para permitir que o loop de
                // poll execute mais uma vez
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        });

        consumer.subscribe(Arrays.asList("foo_topic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("\nChave: " + record.key() + "\n" +
                            "Tópico: " + record.topic() + "\n" +
                            "Partição: " + record.partition() + "\n" +
                            "Offset: " + record.offset() + "\n" +
                            "Timestamp: " + record.timestamp() + "\n" +
                            "Valor: " + record.value() + "\n");
                }
            }
        } catch (WakeupException e) {
            logger.info("Shutting down the consumer");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            // Fecha a conexão do consumer, também commitando os offsets
            consumer.close();
        }
    }
}
