package yandex.practicum.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import yandex.practicum.kafka.model.OrderMessage;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class SingleMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(SingleMessageConsumer.class);

    private final ObjectMapper objectMapper;
    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final long pollTimeoutMs;

    public SingleMessageConsumer(
            ObjectMapper objectMapper,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${app.kafka.topic}") String topic,
            @Value("${app.kafka.single-group-id}") String groupId,
            @Value("${app.kafka.poll-timeout-ms:1000}") long pollTimeoutMs
    ) {
        this.objectMapper = objectMapper;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.pollTimeoutMs = pollTimeoutMs;
    }

    public void consume(AtomicBoolean running) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())) {
            consumer.subscribe(List.of(topic));
            log.info("SingleMessageConsumer started. groupId={}, topic={}", groupId, topic);

            while (running.get()) {
                var records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            }
        } catch (WakeupException exception) {
            log.info("SingleMessageConsumer wakeup signal received, shutting down");
        } catch (Exception exception) {
            if (!running.get()) {
                log.info("SingleMessageConsumer stopped");
                return;
            }
            log.error("SingleMessageConsumer stopped due to error", exception);
        }
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // The task requires automatic offset commit for single-message flow.
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            OrderMessage message = objectMapper.readValue(record.value(), OrderMessage.class);
            log.info("SingleMessageConsumer received: {}", message);
        } catch (Exception exception) {
            log.error("SingleMessageConsumer deserialization error. rawValue={}", record.value(), exception);
        }
    }
}
