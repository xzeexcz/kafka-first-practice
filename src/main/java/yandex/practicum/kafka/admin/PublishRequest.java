package yandex.practicum.kafka.admin;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.math.BigDecimal;

public record PublishRequest(
        String topic,
        @PositiveOrZero Integer partition,
        String key,
        @NotBlank String orderId,
        @NotBlank String customerId,
        @NotNull @Positive BigDecimal amount
) {

}
