package org.example.temporal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowNotFoundException;
import org.example.model.SagaResponseEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@ConditionalOnProperty(name = "temporal.enabled", havingValue = "true")
public class TemporalSagaResponseListener {

    private final ObjectMapper objectMapper;
    private final WorkflowClient workflowClient;

    public TemporalSagaResponseListener(ObjectMapper objectMapper,
                                        WorkflowClient workflowClient) {
        this.objectMapper = objectMapper;
        this.workflowClient = workflowClient;
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
            if (response.getSagaId() == null || !response.getSagaId().startsWith("temporal-")
                    || response.getStep() == null || response.getEventId() == null
                    || response.getEventId().isBlank() || response.getStatus() == null
                    || response.getEventVersion() == null || response.getEventVersion() != 1
                    || response.getCommandId() == null || response.getCommandId().isBlank()
                    || response.getOperation() == null) {
                return;
            }
            String step = normalizeStep(response.getStep());
            if (step == null) {
                return;
            }
            String operation = response.getOperation();
            if (!"EXECUTE".equals(operation) && !"COMPENSATE".equals(operation)
                    || !TemporalCommandIds.forStep(response.getSagaId(), step, operation)
                    .equals(response.getCommandId())) {
                return;
            }
            StepOutcomeSignal outcome = new StepOutcomeSignal(
                    response.getEventId(),
                    response.getSagaId(),
                    step,
                    operation,
                    response.getCommandId(),
                    response.getStatus());
            CreateProgramWorkflow workflow = workflowClient.newWorkflowStub(
                    CreateProgramWorkflow.class,
                    "fitflow-create-program-"
                            + response.getSagaId().substring("temporal-".length()));
            try {
                workflow.receiveOutcome(outcome);
            } catch (WorkflowNotFoundException e) {
                log.info("Ignoring late Temporal outcome for completed or expired workflowId={}",
                        "fitflow-create-program-"
                                + response.getSagaId().substring("temporal-".length()));
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid Temporal downstream response", e);
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
        return null;
    }
}
