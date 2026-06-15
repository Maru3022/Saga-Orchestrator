package org.example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    // Saga orchestrator topics
    @Bean
    public NewTopic sagaUserCreatedTopic() {
        return TopicBuilder.name("saga.user.created").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNotificationSendTopic() {
        return TopicBuilder.name("saga.notification.send").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNotificationResponseTopic() {
        return TopicBuilder.name("saga.notification.response").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaCabinetCreateTopic() {
        return TopicBuilder.name("saga.cabinet.create").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaCabinetResponseTopic() {
        return TopicBuilder.name("saga.cabinet.response").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNutritionCalculateTopic() {
        return TopicBuilder.name("saga.nutrition.calculate").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNutritionResponseTopic() {
        return TopicBuilder.name("saga.nutrition.response").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNotificationCommandTopic() {
        return TopicBuilder.name("saga-notification-command").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNutritionCommandTopic() {
        return TopicBuilder.name("saga-nutrition-command").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNotificationResponseHyphenTopic() {
        return TopicBuilder.name("saga-notification-response").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNutritionResponseHyphenTopic() {
        return TopicBuilder.name("saga-nutrition-response").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaTrainsResponseTopic() {
        return TopicBuilder.name("saga-trains-response").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaUserResponseTopic() {
        return TopicBuilder.name("saga-user-response").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaTrainsCommandTopic() {
        return TopicBuilder.name("saga-trains-command").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaUserCommandTopic() {
        return TopicBuilder.name("saga-user-command").partitions(3).replicas(1).build();
    }

    // Compensation topics
    @Bean
    public NewTopic sagaNotificationCompensateTopic() {
        return TopicBuilder.name("saga.notification.compensate").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaCabinetCompensateTopic() {
        return TopicBuilder.name("saga.cabinet.compensate").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaUserCompensateTopic() {
        return TopicBuilder.name("saga.user.compensate").partitions(3).replicas(1).build();
    }

    // Dead Letter Topics
    @Bean
    public NewTopic sagaNotificationSendDltTopic() {
        return TopicBuilder.name("saga.notification.send.DLT").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaCabinetCreateDltTopic() {
        return TopicBuilder.name("saga.cabinet.create.DLT").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNutritionCalculateDltTopic() {
        return TopicBuilder.name("saga.nutrition.calculate.DLT").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaUserCreatedDltTopic() {
        return TopicBuilder.name("saga.user.created.DLT").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNotificationResponseDltTopic() {
        return TopicBuilder.name("saga.notification.response.DLT").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaCabinetResponseDltTopic() {
        return TopicBuilder.name("saga.cabinet.response.DLT").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNutritionResponseDltTopic() {
        return TopicBuilder.name("saga.nutrition.response.DLT").partitions(3).replicas(1).build();
    }
}
