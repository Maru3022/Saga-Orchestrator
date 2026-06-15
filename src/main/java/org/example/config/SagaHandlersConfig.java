package org.example.config;

import org.example.service.handlers.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.List;

@Configuration
public class SagaHandlersConfig {

    @Bean
    @Primary
    public List<StepHandler> sagaStepHandlers(
            UserStepHandler userStepHandler,
            NotificationStepHandler notificationStepHandler,
            TrainsStepHandler trainsStepHandler,
            NutritionStepHandler nutritionStepHandler) {
        return List.of(
                userStepHandler,
                notificationStepHandler,
                trainsStepHandler,
                nutritionStepHandler
        );
    }
}