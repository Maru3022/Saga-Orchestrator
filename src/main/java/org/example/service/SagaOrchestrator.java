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
    private final List<StepHandler> handlers;

    public SagaOrchestrator(SagaStateRepository repository, List<StepHandler> handlers) {
        this.repository = repository;
        this.handlers = handlers;
    }

    public SagaState startSaga(String sagaType, Map<String, Object> payload) {
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

        return state;
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
                log.error("Error during rollback in handler: {}", handler.getStepName(), e);
            }
        }
    }
}