package org.example.temporal;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.model.SagaResponseEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TemporalSagaResponseListener {

    private final ObjectMapper objectMapper;
    private final TemporalStepOutcomeRegistry outcomeRegistry;

    public TemporalSagaResponseListener(ObjectMapper objectMapper,
                                        TemporalStepOutcomeRegistry outcomeRegistry) {
        this.objectMapper = objectMapper;
        this.outcomeRegistry = outcomeRegistry;
    }

    @KafkaListener(
            topics = {
                    "#{@sagaTopicsProperties.notificationResponseHyphen}",
                    "#{@sagaTopicsProperties.trainsResponse}",
                    "#{@sagaTopicsProperties.nutritionResponseHyphen}",
                    "#{@sagaTopicsProperties.userResponse}"
            },
            containerFactory = "sagaKafkaListenerContainerFactory",
            groupId = "temporal-outcome-group")
    public void onResponse(String rawMessage) {
        try {
            SagaResponseEvent response = objectMapper.readValue(rawMessage, SagaResponseEvent.class);
            if (response.getSagaId() == null || response.getStep() == null) {
                return;
            }
            boolean success = "SUCCESS".equalsIgnoreCase(response.getStatus());
            outcomeRegistry.complete(
                    response.getSagaId(),
                    normalizeStep(response.getStep()),
                    new StepOutcome(success, response.getStatus()));
        } catch (Exception ignored) {
            throw new IllegalArgumentException("Invalid Temporal downstream response", ignored);
        }
    }

    private String normalizeStep(String step) {
        if ("TRAINING".equalsIgnoreCase(step) || "TRAINS".equalsIgnoreCase(step)
                || "TRAIN".equalsIgnoreCase(step)) {
            return "TRAINING";
        }
        if ("NOTIFICATION".equalsIgnoreCase(step)) {
            return "NOTIFICATION";
        }
        if ("NUTRITION".equalsIgnoreCase(step)) {
            return "NUTRITION";
        }
        return step.toUpperCase();
    }
}
