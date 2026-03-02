package yandex.practicum.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import yandex.practicum.kafka.model.OrderMessage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class BatchMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(BatchMessageConsumer.class);

    private final ObjectMapper objectMapper;
    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final int batchSize;
    private final int fetchMinBytes;
    private final int fetchMaxWaitMs;
    private final long pollTimeoutMs;

    public BatchMessageConsumer(
            ObjectMapper objectMapper,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${app.kafka.topic}") String topic,
            @Value("${app.kafka.batch-group-id}") String groupId,
            @Value("${app.kafka.batch-size:10}") int batchSize,
            @Value("${app.kafka.fetch-min-bytes:1024}") int fetchMinBytes,
            @Value("${app.kafka.fetch-max-wait-ms:3000}") int fetchMaxWaitMs,
            @Value("${app.kafka.poll-timeout-ms:1000}") long pollTimeoutMs
    ) {
        this.objectMapper = objectMapper;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.batchSize = batchSize;
        this.fetchMinBytes = fetchMinBytes;
        this.fetchMaxWaitMs = fetchMaxWaitMs;
        this.pollTimeoutMs = pollTimeoutMs;
    }

    public void consume(AtomicBoolean running) {
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())) {
            consumer.subscribe(List.of(topic));
            log.info("BatchMessageConsumer started. groupId={}, topic={}, batchSize={}", groupId, topic, batchSize);

            while (running.get()) {
                var records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
                if (!records.isEmpty()) {
                    records.forEach(buffer::add);
                }

                while (buffer.size() >= batchSize) {
                    processAndCommit(consumer, buffer.subList(0, batchSize));
                    buffer.subList(0, batchSize).clear();
                }
            }

            if (!buffer.isEmpty()) {
                processAndCommit(consumer, buffer);
            }
        } catch (WakeupException exception) {
            log.info("BatchMessageConsumer wakeup signal received, shutting down");
        } catch (Exception exception) {
            if (!running.get()) {
                log.info("BatchMessageConsumer stopped");
                return;
            }
            log.error("BatchMessageConsumer stopped due to error", exception);
        }
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        return properties;
    }

    private void processAndCommit(KafkaConsumer<String, String> consumer, List<ConsumerRecord<String, String>> batchView) {
        List<ConsumerRecord<String, String>> batch = new ArrayList<>(batchView);
        processBatch(batch);

        try {
            consumer.commitSync(offsetsForBatch(batch));
            log.info("BatchMessageConsumer committed batch. size={}", batch.size());
        } catch (Exception exception) {
            log.error("BatchMessageConsumer commit error", exception);
        }
    }

    private void processBatch(List<ConsumerRecord<String, String>> batch) {
        for (ConsumerRecord<String, String> record : batch) {
            try {
                OrderMessage message = objectMapper.readValue(record.value(), OrderMessage.class);
                log.info("BatchMessageConsumer received: {}", message);
            } catch (Exception exception) {
                log.error("BatchMessageConsumer deserialization error. rawValue={}", record.value(), exception);
            }
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> offsetsForBatch(List<ConsumerRecord<String, String>> batch) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (ConsumerRecord<String, String> record : batch) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            offsets.put(topicPartition, new OffsetAndMetadata(record.offset() + 1));
        }
        return offsets;
    }
}
