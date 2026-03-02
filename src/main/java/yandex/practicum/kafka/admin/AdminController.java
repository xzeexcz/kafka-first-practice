package yandex.practicum.kafka.admin;

import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import yandex.practicum.kafka.model.OrderMessage;
import yandex.practicum.kafka.producer.OrderMessageProducer;

import java.time.Clock;
import java.time.Instant;

@RestController
@RequestMapping("/admin")
public class AdminController {

    private final OrderMessageProducer producer;
    private final String defaultTopic;
    private final Clock clock;

    public AdminController(
            OrderMessageProducer producer,
            @Value("${app.kafka.topic}") String defaultTopic,
            Clock clock
    ) {
        this.producer = producer;
        this.defaultTopic = defaultTopic;
        this.clock = clock;
    }

    @PostMapping("/publish")
    public ResponseEntity<PublishResponse> publish(@Valid @RequestBody PublishRequest request) {
        String topic = (request.topic() == null || request.topic().isBlank()) ? defaultTopic : request.topic();
        String key = (request.key() == null || request.key().isBlank()) ? request.orderId() : request.key();

        OrderMessage message = new OrderMessage(
                request.orderId(),
                request.customerId(),
                request.amount(),
                Instant.now(clock)
        );

        producer.publish(topic, request.partition(), key, message);

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(new PublishResponse(
                "accepted",
                topic,
                request.partition(),
                key,
                message
        ));
    }
}
