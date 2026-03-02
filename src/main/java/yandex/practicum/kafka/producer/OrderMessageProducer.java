package yandex.practicum.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import yandex.practicum.kafka.model.OrderMessage;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderMessageProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OrderMessageProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publish(String topic, Integer partition, String key, OrderMessage message) {
        String payload = serialize(message);
        log.info("Publishing message. topic={}, key={}, partition={}, payload={}", topic, key, partition, payload);

        CompletableFuture<SendResult<String, String>> resultFuture = send(topic, partition, key, payload);
        resultFuture.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Kafka send failed. topic={}, key={}", topic, key, exception);
                return;
            }
            if (result != null) {
                log.info(
                        "Kafka send success. topic={}, partition={}, offset={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset()
                );
            }
        });
    }

    private String serialize(OrderMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException exception) {
            log.error("Serialization error for message={}", message, exception);
            throw new IllegalArgumentException("Cannot serialize message", exception);
        }
    }

    private CompletableFuture<SendResult<String, String>> send(String topic, Integer partition, String key, String payload) {
        if (partition != null) {
            return kafkaTemplate.send(topic, partition, key, payload);
        }
        if (key != null && !key.isBlank()) {
            return kafkaTemplate.send(topic, key, payload);
        }
        return kafkaTemplate.send(topic, payload);
    }
}
