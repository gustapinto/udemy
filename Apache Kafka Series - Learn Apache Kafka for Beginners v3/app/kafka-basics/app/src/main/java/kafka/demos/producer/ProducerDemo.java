package kafka.demos.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
    public static void run(String[] args) {
        // Cria as propriedades do produtor
        Properties properties = new Properties();

        // Consigura o bootstrap server usado para conectar o produtor
        // System.getenv(...) -> Obtém uma variável de ambiente como String
        properties.setProperty("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVER"));

        // Configura os serializadores de chaves e valores, fazendo com que o
        // produtor espere Strings para as chaves e valores
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Inicializa um kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Inicializa um producer record, passando o nome do tópico no qual o valor
        // vai ser enviado e o valor em si
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String,String>("foo_topic", "Hello World!");

        // Envia uma mensagem
        producer.send(producerRecord);

        // Bloqueia a execução e envia todos os dados até finalizar
        producer.flush();

        // Fecha o produtor
        producer.close();
    }
}
