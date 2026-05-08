package org.example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean(name = "saga-trains-command")
    public NewTopic trainsCommandTopic() {
        return TopicBuilder.name("saga-trains-command").partitions(3).replicas(1).build();
    }

    @Bean(name = "saga-nutrition-command")
    public NewTopic nutritionCommandTopic(){
        return TopicBuilder.name("saga-nutrition-command").partitions(3).replicas(1).build();
    }

    @Bean(name = "saga-notification-command")
    public NewTopic notificationCommandTopic(){
        return TopicBuilder.name("saga-notification-command").partitions(3).replicas(1).build();
    }

    @Bean(name = "saga-trains-response")
    public NewTopic trainsResponseTopic(){
        return TopicBuilder.name("saga-trains-response").partitions(3).replicas(1).build();
    }

    @Bean(name = "saga-nutrition-response")
    public NewTopic nutritionResponseTopic(){
        return TopicBuilder.name("saga-nutrition-response").partitions(3).replicas(1).build();
    }

    @Bean(name = "saga-notification-response")
    public NewTopic notificationResponseTopic(){
        return TopicBuilder.name("saga-notification-response").partitions(3).replicas(1).build();
    }
}
