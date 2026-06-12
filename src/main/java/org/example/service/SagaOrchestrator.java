package org.example.service;

import lombok.extern.slf4j.Slf4j;
import org.example.model.SagaEvent;
import org.example.model.SagaState;
import org.example.repository.SagaStateRepository;
import org.example.service.handlers.StepHandler;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class SagaOrchestrator {

    public static final String SAGA_TYPE = "PROGRAM_CREATION";

    private final SagaStateRepository repository;
    private final List<StepHandler> handlers;

    public SagaOrchestrator(SagaStateRepository repository, List<StepHandler> handlers) {
        this.repository = repository;
        this.handlers = handlers;
    }

    public void startSaga(String sagaType, Map<String, Object> payload) {
        log.info("Starting saga of type: {}", sagaType);
        SagaState state = new SagaState(sagaType, payload);
        repository.save(state);

        if (!handlers.isEmpty()) {
            StepHandler firstHandler = handlers.get(0);
            state.setCurrentStep(firstHandler.getStepName());
            repository.save(state);

            SagaEvent event = new SagaEvent(state.getSagaId(), firstHandler.getStepName(), "START", payload);
            firstHandler.processForward(event);
        }
    }

    public void handleEvent(SagaEvent event) {
        if (event == null || event.getSagaId() == null) {
            log.error("Invalid saga event received");
            return;
        }

        log.info("Handling saga event for sagaId: {}, step: {}, status: {}",
                event.getSagaId(), event.getStepName(), event.getStatus());

        SagaState state = repository.findById(event.getSagaId()).orElse(null);
        if (state == null) {
            log.error("Saga state not found for sagaId: {}", event.getSagaId());
            return;
        }

        if ("SUCCESS".equals(event.getStatus())) {
            int currentIndex = -1;
            for (int i = 0; i < handlers.size(); i++) {
                if (handlers.get(i).getStepName().equalsIgnoreCase(event.getStepName())) {
                    currentIndex = i;
                    break;
                }
            }

            if (currentIndex != -1 && currentIndex + 1 < handlers.size()) {
                StepHandler nextHandler = handlers.get(currentIndex + 1);
                state.setCurrentStep(nextHandler.getStepName());
                repository.save(state);

                SagaEvent nextEvent = new SagaEvent(state.getSagaId(), nextHandler.getStepName(), "START", event.getData());
                nextHandler.processForward(nextEvent);
            } else {
                state.setStatus(SagaState.STATUS_COMPLETED);
                repository.save(state);
                log.info("Saga {} successfully completed!", state.getSagaId());
            }
        } else if ("FAILED".equals(event.getStatus()) || "ROLLBACK".equals(event.getStatus())) {
            failAndRollback(state, "Saga step failed: " + event.getStepName());
        }
    }

    public void failAndRollback(SagaState state, String reason) {
        if (state == null) return;

        log.warn("Rolling back saga {} due to: {}", state.getSagaId(), reason);
        state.setStatus(SagaState.STATUS_COMPENSATING);
        state.setFailureReason(reason);
        repository.save(state);

        int startIndex = handlers.size() - 1;
        for (int i = 0; i < handlers.size(); i++) {
            if (handlers.get(i).getStepName().equalsIgnoreCase(state.getCurrentStep())) {
                startIndex = i;
                break;
            }
        }

        for (int i = startIndex; i >= 0; i--) {
            StepHandler handler = handlers.get(i);
            SagaEvent rollbackEvent = new SagaEvent(
                    state.getSagaId(), handler.getStepName(), "ROLLBACK", state.getPayload()
            );
            try {
                handler.processRollback(rollbackEvent);
            } catch (Exception e) {
                log.error("Error during rollback step {}", handler.getStepName(), e);
            }
        }
    }
}
