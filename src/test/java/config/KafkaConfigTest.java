package org.example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {
        "saga-trains-command", "saga-nutrition-command", "saga-notification-command",
        "saga-trains-response", "saga-nutrition-response", "saga-notification-response"
})
public class KafkaConfigTest {

    @Autowired
    private Map<String, NewTopic> topics;

    @Test
    void shouldCreateCommandTopics(){
        assertThat(topics).containsKey("saga-trains-command");
        assertThat(topics.get("saga-trains-command").name()).isEqualTo("saga-trains-command");
        assertThat(topics.get("saga-trains-command").numPartitions()).isEqualTo(3);
    }

    @Test
    void shouldCreateResponseTopics(){
        assertThat(topics).containsKey("saga-trains-response");
        assertThat(topics).containsKey("saga-nutrition-response");
        assertThat(topics).containsKey("saga-notification-response");
    }
}
