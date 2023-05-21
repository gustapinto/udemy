package kafka_opensearch;

import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

import kafka_opensearch.factories.KafkaConsumerFactory;
import kafka_opensearch.factories.OpensearchRestHighLevelClientFactory;

public class App {
    public static final String OPENSEARCH_HOST = System.getenv("OPENSEARCH_HOST");
    public static final String KAFKA_BOOTSTRAP_SERVER = System.getenv("KAFKA_BOOTSTRAP_SERVER");
    public static final String OPENSEARCH_INDEX = "wikimedia";
    public static final String KAFKA_TOPIC = "wikimedia.recentchange";

    public static Logger logger = LoggerFactory.getLogger(App.class);

    private static String getIdFromRecord(ConsumerRecord<String, String> record) {
        return JsonParser.parseString(record.value())
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws Exception {
        RestHighLevelClient openSearchClient = OpensearchRestHighLevelClientFactory.make(OPENSEARCH_HOST);
        KafkaConsumer<String, String> kafkaConsumer = KafkaConsumerFactory.make(KAFKA_BOOTSTRAP_SERVER, "java-app");

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Exiting Kafka Consumer...");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        });

        // Cria o indice caso ele não exista
        //
        // try (...) {} fecha todos os itens passados caso haja uma exceção
        try (openSearchClient; kafkaConsumer) {
            GetIndexRequest getIndexRequest = new GetIndexRequest(OPENSEARCH_INDEX);

            // <opensearch client>.indices() -> Provê acesso aos indices do Opensearch
            // <index client>.exists() -> Valida se um indíce existe
            boolean indexAlreadyExists = openSearchClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);

            if (!indexAlreadyExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(OPENSEARCH_INDEX);

                // <index client>.create() -> Cria um novo indíce
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

                logger.info("Created Opensearch index");
            }

            kafkaConsumer.subscribe(Arrays.asList(KAFKA_TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
                Integer recordsCount = records.count();

                logger.info("Received " + recordsCount + " records");

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // <index request>.source() -> Adiciona um corpo ao documento enviado na
                        // requisição
                        // <index request>.id() -> Adiciona um id ao documento enviado na requisição
                        IndexRequest indexRequest = new IndexRequest(OPENSEARCH_INDEX)
                                .source(record.value(), XContentType.JSON)
                                .id(getIdFromRecord(record));

                        // <bulk request>.add() -> Adiciona uma nova requisição ao lote
                        bulkRequest.add(indexRequest);
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                }

                // <bulk request>.numberOfActions() -> Retorna a quantidade de
                // requisições acumuladas no lote
                if (bulkRequest.numberOfActions() > 0) {
                    // <bulk request>.bulk() -> Envia as requisições acumuladas
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage());
                    }

                    logger.info("Inserted " + bulkResponse.getItems().length + " records into Opensearch...");

                    // <kafka client>.commitSync() -> Commita sincronamente os offsets após
                    // consumi-los, só é necessário se usar enabled.auto.commit = false
                    kafkaConsumer.commitSync();

                    logger.info("Published all offsets");
                }

            }
        } catch (WakeupException e) {
            logger.info("Shutting down the consumer");
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
