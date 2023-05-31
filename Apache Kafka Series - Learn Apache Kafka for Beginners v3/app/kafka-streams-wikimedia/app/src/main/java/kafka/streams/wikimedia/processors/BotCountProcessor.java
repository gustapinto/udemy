package kafka.streams.wikimedia.processors;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BotCountProcessor {
    private static final String BOT_COUNT_STORE = "bot-count-score";
    private static final String BOT_COUNT_TOPIC = "wikimedia.stats.bots";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private KStream<String, String> inputStream;

    public BotCountProcessor(KStream<String, String> inputStream) {
        this.inputStream = inputStream;
    }

    public void process() {
        this.inputStream.mapValues((String value) -> {
            try {
                JsonNode jsonNode = MAPPER.readTree(value);

                if (jsonNode.get("bot").asBoolean()) {
                    return "bot";
                }

                return "non-bot";
            } catch (IOException e) {
                return "";
            }
        })
        .groupBy((String key, String value) -> value)
        .count(Materialized.as(BOT_COUNT_STORE))
        .toStream()
        .mapValues((String readOnlyKey, Long value) -> {
            Map<String, Long> map = Map.of(String.valueOf(readOnlyKey), value);

            try {
                return MAPPER.writeValueAsString(map);
            } catch (JsonProcessingException e) {
                return null;
            }
        })
        .to(BOT_COUNT_TOPIC);
    }
}
