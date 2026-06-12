package org.example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic userCreatedTopic() {
        return TopicBuilder.name("user.created").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic userCommandTopic() {
        return TopicBuilder.name("saga-user-command").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic userResponseTopic() {
        return TopicBuilder.name("saga-user-response").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic trainsCommandTopic() {
        return TopicBuilder.name("saga-trains-command").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic nutritionCommandTopic() {
        return TopicBuilder.name("saga-nutrition-command").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic notificationCommandTopic() {
        return TopicBuilder.name("saga-notification-command").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic trainsResponseTopic() {
        return TopicBuilder.name("saga-trains-response").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic nutritionResponseTopic() {
        return TopicBuilder.name("saga-nutrition-response").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic notificationResponseTopic() {
        return TopicBuilder.name("saga-notification-response").partitions(3).replicas(1).build();
    }
}