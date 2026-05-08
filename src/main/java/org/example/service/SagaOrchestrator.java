package org.example.service;

import org.example.model.SagaDefinition;
import org.example.model.SagaEvent;
import org.example.model.SagaState;
import org.example.repository.SagaStateRepository;
import org.example.service.handlers.StepHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SagaOrchestrator {
    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    @Autowired
    private Map<String, StepHandler> stepHandlers;
    @Autowired
    private SagaStateRepository sagaStateRepository;
    @Autowired
    private KafkaTemplate<String, SagaEvent> kafkaTemplate;

    private final Map<String, SagaDefinition> sagaDefinitions = new ConcurrentHashMap<>();

    public SagaOrchestrator() {
        List<String> forward = Arrays.asList("TRAINS", "NUTRITION", "NOTIFICATION");
        List<String> rollback = Arrays.asList("NOTIFICATION", "NUTRITION", "TRAINS");
        sagaDefinitions.put("CREATE_PROGRAM", new SagaDefinition("CREATE_PROGRAM", forward, rollback));
    }

    public SagaState startSaga(String sagaType, Map<String, Object> payload) {
        SagaDefinition definition = sagaDefinitions.get(sagaType);
        if (definition == null) {
            throw new IllegalArgumentException("Unknown saga type: " + sagaType);
        }
        SagaState state = new SagaState(sagaType, payload);
        state.setCurrentStep("STARTED");
        sagaStateRepository.save(state);
        log.info("Saga {} started with id {}", sagaType, state.getSagaId());

        proceedToNextStep(state, definition);
        return state;
    }

    private void proceedToNextStep(SagaState state, SagaDefinition definition) {
        List<String> forwardSteps = definition.getForwardSteps();
        String current = state.getCurrentStep();
        int idx = forwardSteps.indexOf(current);
        if (idx == forwardSteps.size() - 1) {
            state.setCurrentStep("COMPLETED");
            sagaStateRepository.save(state);
            log.info("Saga {} completed successfully", state.getSagaId());
            return;
        }
        String nextStep = forwardSteps.get(idx + 1);
        state.setCurrentStep(nextStep);
        sagaStateRepository.save(state);

        StepHandler handler = findHandler(nextStep);
        SagaEvent event = new SagaEvent(state.getSagaId(), nextStep, "EXECUTE", state.getPayload());
        handler.processForward(event);
    }

    @KafkaListener(topics = {
            "saga-trains-response",
            "saga-nutrition-response",
            "saga-notification-response"
    }, groupId = "saga-orchestrator")
    public void onServiceResponse(SagaEvent event) {
        log.info("Received response: sagaId={}, step={}, status={}",
                event.getSagaId(), event.getStep(), event.getStatus());
        Optional<SagaState> optState = sagaStateRepository.findById(event.getSagaId());
        if (optState.isEmpty()) {
            log.warn("Saga state not found for id {}", event.getSagaId());
            return;
        }
        SagaState state = optState.get();
        SagaDefinition definition = sagaDefinitions.get(state.getSagaType());
        if (definition == null) {
            log.error("Saga definition not found for type {}", state.getSagaType());
            return;
        }

        if ("SUCCESS".equals(event.getStatus())) {
            if (event.getData() != null) {
                state.getPayload().put(event.getStep() + "_result", event.getData());
            }
            proceedToNextStep(state, definition);
        } else {
            log.error("Step {} failed for saga {}. Starting compensation.", event.getStep(), event.getSagaId());
            state.setCurrentStep("FAILED");
            state.setFailureReason(event.getData() != null ? event.getData().toString() : "unknown");
            sagaStateRepository.save(state);
            rollback(state, definition, event.getStep());
        }
    }

    private void rollback(SagaState state, SagaDefinition definition, String failedStep) {
        List<String> rollbackSteps = definition.getRollbackSteps();
        boolean startCompensation = false;
        for (String step : rollbackSteps) {
            if (step.equals(failedStep)) {
                startCompensation = true;
                continue;
            }
            if (startCompensation) {
                StepHandler handler = findHandler(step);
                SagaEvent rollbackEvent = new SagaEvent(state.getSagaId(), step, "ROLLBACK", state.getPayload());
                handler.processRollback(rollbackEvent);
            }
        }
        sagaStateRepository.delete(state.getSagaId());
    }

    private StepHandler findHandler(String step) {
        return stepHandlers.values().stream()
                .filter(h -> h.getStepName().equals(step))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Handler not found for step " + step));
    }
}