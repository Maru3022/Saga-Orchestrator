package org.example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    private final SagaTopicsProperties topics;

    public KafkaConfig(SagaTopicsProperties topics) {
        this.topics = topics;
    }

    // Saga orchestrator topics
    @Bean
    public NewTopic sagaUserCreatedTopic() {
        return TopicBuilder.name(topics.getUserCreated()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNotificationSendTopic() {
        return TopicBuilder.name(topics.getNotificationSend()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNotificationResponseTopic() {
        return TopicBuilder.name(topics.getNotificationResponse()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaCabinetCreateTopic() {
        return TopicBuilder.name(topics.getCabinetCreate()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaCabinetResponseTopic() {
        return TopicBuilder.name(topics.getCabinetResponse()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNutritionCalculateTopic() {
        return TopicBuilder.name(topics.getNutritionCalculate()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNutritionResponseTopic() {
        return TopicBuilder.name(topics.getNutritionResponse()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNotificationCommandTopic() {
        return TopicBuilder.name(topics.getNotificationCommand()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNutritionCommandTopic() {
        return TopicBuilder.name(topics.getNutritionCommand()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNotificationResponseHyphenTopic() {
        return TopicBuilder.name(topics.getNotificationResponseHyphen()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNutritionResponseHyphenTopic() {
        return TopicBuilder.name(topics.getNutritionResponseHyphen()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaTrainsResponseTopic() {
        return TopicBuilder.name(topics.getTrainsResponse()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaUserResponseTopic() {
        return TopicBuilder.name(topics.getUserResponse()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaTrainsCommandTopic() {
        return TopicBuilder.name(topics.getTrainsCommand()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaUserCommandTopic() {
        return TopicBuilder.name(topics.getUserCommand()).partitions(3).replicas(1).build();
    }

    // Compensation topics
    @Bean
    public NewTopic sagaNotificationCompensateTopic() {
        return TopicBuilder.name(topics.getNotificationCompensate()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaCabinetCompensateTopic() {
        return TopicBuilder.name(topics.getCabinetCompensate()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaUserCompensateTopic() {
        return TopicBuilder.name(topics.getUserCompensate()).partitions(3).replicas(1).build();
    }

    // Dead Letter Topics
    @Bean
    public NewTopic sagaNotificationSendDltTopic() {
        return TopicBuilder.name(topics.getNotificationSendDlt()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaCabinetCreateDltTopic() {
        return TopicBuilder.name(topics.getCabinetCreateDlt()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNutritionCalculateDltTopic() {
        return TopicBuilder.name(topics.getNutritionCalculateDlt()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaUserCreatedDltTopic() {
        return TopicBuilder.name(topics.getUserCreatedDlt()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNotificationResponseDltTopic() {
        return TopicBuilder.name(topics.getNotificationResponseDlt()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaCabinetResponseDltTopic() {
        return TopicBuilder.name(topics.getCabinetResponseDlt()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic sagaNutritionResponseDltTopic() {
        return TopicBuilder.name(topics.getNutritionResponseDlt()).partitions(3).replicas(1).build();
    }
}
