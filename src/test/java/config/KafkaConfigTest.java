package org.example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    private Set<String> topicNames() {
        return topics.values().stream()
                .map(NewTopic::name)
                .collect(Collectors.toSet());
    }

    @Test
    void shouldCreateCommandTopics() {
        Set<String> names = topicNames();

        assertThat(names).contains(
                "saga-trains-command",
                "saga-nutrition-command",
                "saga-notification-command",
                "saga-user-command"
        );

        NewTopic trainsCommand = findByName("saga-trains-command");
        assertThat(trainsCommand.numPartitions()).isEqualTo(3);
        assertThat(trainsCommand.replicationFactor()).isEqualTo((short) 1);
    }

    @Test
    void shouldCreateResponseTopics() {
        Set<String> names = topicNames();

        assertThat(names).contains(
                "saga-trains-response",
                "saga-nutrition-response",
                "saga-notification-response",
                "saga-user-response"
        );
    }

    @Test
    void shouldCreateUserCreatedTopic() {
        assertThat(topicNames()).contains("user.created");
    }

    @Test
    void shouldNotHaveDuplicateOrBlankTopicNames() {
        var names = topics.values().stream().map(NewTopic::name).toList();

        assertThat(names).doesNotHaveDuplicates();
        assertThat(names).allSatisfy(name -> assertThat(name).isNotBlank());
    }

    private NewTopic findByName(String topicName) {
        return topics.values().stream()
                .filter(t -> t.name().equals(topicName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Topic not found: " + topicName));
    }
}