package yandex.practicum.kafka.admin;

import yandex.practicum.kafka.model.OrderMessage;

public record PublishResponse(
        String status,
        String topic,
        Integer partition,
        String key,
        OrderMessage message
) {
}
