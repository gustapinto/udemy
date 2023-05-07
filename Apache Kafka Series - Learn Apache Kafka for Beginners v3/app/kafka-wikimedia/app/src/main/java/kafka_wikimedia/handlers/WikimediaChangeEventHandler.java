package kafka_wikimedia.handlers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;

public class WikimediaChangeEventHandler implements BackgroundEventHandler {
    private KafkaProducer<String, String> kafkaProducer;
    private String kafkaTopic;

    private final Logger logger = LoggerFactory.getLogger(WikimediaChangeEventHandler.class.getName());

    public WikimediaChangeEventHandler(KafkaProducer<String, String> kafkaProducer, String kafkaTopic) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
    }

    @Override
    public void onOpen() throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void onClosed() throws Exception {
        this.kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        this.logger.info("Sending event data to producer: " + messageEvent.getData());

        ProducerRecord<String, String> record = new ProducerRecord<String,String>(this.kafkaTopic, messageEvent.getData());

        this.kafkaProducer.send(record);
    }

    @Override
    public void onComment(String comment) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void onError(Throwable t) {
        this.logger.error("Error when sendidn event: " + t.getMessage());
    }
}
