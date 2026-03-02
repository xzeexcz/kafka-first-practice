package yandex.practicum.kafka.model;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderMessage(
        String orderId,
        String customerId,
        BigDecimal amount,
        Instant createdAt
) {
}
