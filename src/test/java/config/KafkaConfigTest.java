package config;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
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
