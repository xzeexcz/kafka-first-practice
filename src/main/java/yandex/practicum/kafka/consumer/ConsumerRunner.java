package yandex.practicum.kafka.consumer;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ConsumerRunner {

    private static final Logger log = LoggerFactory.getLogger(ConsumerRunner.class);

    private final SingleMessageConsumer singleMessageConsumer;
    private final BatchMessageConsumer batchMessageConsumer;
    private final boolean consumersEnabled;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newFixedThreadPool(2, namedThreadFactory());

    public ConsumerRunner(
            SingleMessageConsumer singleMessageConsumer,
            BatchMessageConsumer batchMessageConsumer,
            @Value("${app.kafka.consumers.enabled:true}") boolean consumersEnabled
    ) {
        this.singleMessageConsumer = singleMessageConsumer;
        this.batchMessageConsumer = batchMessageConsumer;
        this.consumersEnabled = consumersEnabled;
    }

    @PostConstruct
    public void start() {
        if (!consumersEnabled) {
            log.info("Consumers are disabled via configuration");
            return;
        }

        running.set(true);
        executor.submit(() -> singleMessageConsumer.consume(running));
        executor.submit(() -> batchMessageConsumer.consume(running));
        log.info("Consumers started");
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Consumers executor did not terminate in time");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            log.warn("Consumer shutdown interrupted", exception);
        }
    }

    private ThreadFactory namedThreadFactory() {
        AtomicInteger counter = new AtomicInteger(1);
        return runnable -> {
            Thread thread = new Thread(runnable, "kafka-consumer-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }
}
