package org.example;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {
        "saga-trains-command", "saga-nutrition-command", "saga-notification-command",
        "saga-trains-response", "saga-nutrition-response", "saga-notification-response"
})
class SagaOrchestratorApplicationTests {

    @Test
    void contextLoads() {
    }
}