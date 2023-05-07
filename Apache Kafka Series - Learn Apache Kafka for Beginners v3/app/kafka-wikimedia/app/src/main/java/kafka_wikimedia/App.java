package kafka_wikimedia;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.background.BackgroundEventSource;

import kafka_wikimedia.handlers.WikimediaChangeEventHandler;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;

public class App {
    public static final String TOPIC = "wikimedia.recentchange";
    public static final URI WIKIMEDIA_URI = URI.create("https://stream.wikimedia.org/v2/stream/recentchange");

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVER"));
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Implementando um produtor "seguro", que é o padrão a partir do Kafka 3
        // porém, em versões anteriores é preciso configurar as propriedades:
        properties.setProperty("enable.idempotence", "true");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", Integer.toString(Integer.MAX_VALUE));

        // Configura propriedades para melhor o desempenho de envio de
        // mensagens do produtor
        properties.setProperty("compression.type", "snappy");
        properties.setProperty("linger.ms", "20");
        properties.setProperty("batch.size", Integer.toString(32 * 1024));

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        BackgroundEventHandler backgroundEventHandler = new WikimediaChangeEventHandler(producer, TOPIC);

        ConnectStrategy connectStrategy = ConnectStrategy.http(App.WIKIMEDIA_URI)
                .connectTimeout(10, TimeUnit.SECONDS);
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(connectStrategy);
        BackgroundEventSource backgroundEventSource = new BackgroundEventSource.Builder(backgroundEventHandler,
                eventSourceBuilder)
                .build();

        // Inicia o consumidor de eventos em uma thread separada
        backgroundEventSource.start();

        // Bloqueia execução do serviço para que a thread e, background possa
        // consumir os dados da stream
        while (true) {
            TimeUnit.MINUTES.sleep(5);
        }
    }
}
