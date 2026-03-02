package yandex.practicum.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "app.kafka.consumers.enabled=false")
class KafkaApplicationTests {

    @Test
    void contextLoads() {
    }

}
