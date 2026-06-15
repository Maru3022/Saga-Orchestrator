package org.example.service;

import lombok.extern.slf4j.Slf4j;
import org.example.model.SagaEvent;
import org.example.model.SagaState;
import org.example.repository.SagaStateRepository;
import org.example.service.handlers.StepHandler;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class SagaOrchestrator {

    public static final String SAGA_TYPE = "PROGRAM_CREATION";

    private final SagaStateRepository repository;
    private final List<StepHandler> sagaStepHandlers;

    public SagaOrchestrator(SagaStateRepository repository,
                            List<StepHandler> sagaStepHandlers) {
        this.repository = repository;
        this.sagaStepHandlers = sagaStepHandlers;
    }

    public SagaState startSaga(String sagaType, Map<String, Object> payload) {
        log.info("Starting saga of type: {}", sagaType);
        SagaState state = new SagaState(sagaType, payload);
        repository.save(state);

        if (sagaStepHandlers.size() > 1) {
            StepHandler firstActive = sagaStepHandlers.get(1);
            state.setCurrentStep(firstActive.getStepName());
            repository.save(state);

            SagaEvent event = new SagaEvent(
                    state.getSagaId(), firstActive.getStepName(), "EXECUTE", payload);
            firstActive.processForward(event);
        }

        return state;
    }

    public void handleEvent(SagaEvent event) {
        if (event == null || event.getSagaId() == null) {
            log.error("Invalid saga event received");
            return;
        }

        log.info("Handling saga event: sagaId={}, step={}, status={}",
                event.getSagaId(), event.getStepName(), event.getStatus());

        SagaState state = repository.findById(event.getSagaId()).orElse(null);
        if (state == null) {
            log.error("Saga state not found for sagaId={}", event.getSagaId());
            return;
        }

        if (SagaState.STATUS_COMPLETED.equals(state.getStatus())
                || SagaState.STATUS_FAILED.equals(state.getStatus())) {
            log.warn("Saga {} already in terminal state {}, ignoring",
                    state.getSagaId(), state.getStatus());
            return;
        }

        if ("SUCCESS".equals(event.getStatus())) {
            handleSuccess(state, event);
        } else if ("FAILED".equals(event.getStatus())) {
            failAndRollback(state, "Step failed: " + event.getStepName());
        }
    }

    private void handleSuccess(SagaState state, SagaEvent event) {
        if (event.getData() != null) {
            state.getPayload().putAll(event.getData());
        }

        int currentIndex = findHandlerIndex(event.getStepName());
        if (currentIndex == -1) {
            log.error("Handler not found for step {}", event.getStepName());
            return;
        }

        if (currentIndex + 1 < sagaStepHandlers.size()) {
            StepHandler next = sagaStepHandlers.get(currentIndex + 1);
            state.setCurrentStep(next.getStepName());
            repository.save(state);

            SagaEvent nextEvent = new SagaEvent(
                    state.getSagaId(), next.getStepName(), "EXECUTE", state.getPayload());
            next.processForward(nextEvent);
        } else {
            state.setStatus(SagaState.STATUS_COMPLETED);
            repository.save(state);
            log.info("Saga {} completed successfully!", state.getSagaId());
        }
    }

    public void failAndRollback(SagaState state, String reason) {
        if (state == null) return;

        log.warn("Rolling back saga {} due to: {}", state.getSagaId(), reason);
        state.setStatus(SagaState.STATUS_COMPENSATING);
        state.setFailureReason(reason);
        repository.save(state);

        int startIndex = findHandlerIndex(state.getCurrentStep());
        if (startIndex == -1) startIndex = sagaStepHandlers.size() - 1;

        for (int i = startIndex; i >= 0; i--) {
            StepHandler handler = sagaStepHandlers.get(i);
            SagaEvent rollback = new SagaEvent(
                    state.getSagaId(), handler.getStepName(), "ROLLBACK", state.getPayload());
            try {
                handler.processRollback(rollback);
            } catch (Exception e) {
                log.error("Error during rollback handler {}: {}",
                        handler.getStepName(), e.getMessage());
            }
        }

        state.setStatus(SagaState.STATUS_FAILED);
        repository.save(state);
    }

    private int findHandlerIndex(String stepName) {
        for (int i = 0; i < sagaStepHandlers.size(); i++) {
            if (sagaStepHandlers.get(i).getStepName().equalsIgnoreCase(stepName)) return i;
        }
        return -1;
    }
}