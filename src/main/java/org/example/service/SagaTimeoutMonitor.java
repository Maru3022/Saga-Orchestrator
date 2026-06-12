package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.model.SagaState;
import org.example.repository.SagaStateRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class SagaTimeoutMonitor {

    private final SagaStateRepository sagaStateRepository;
    private final SagaOrchestrator sagaOrchestrator;

    @Value("${saga.timeout-ms:60000}")
    private long timeoutMs;

    @Scheduled(fixedDelay = 30000)
    public void checkStuckSagas() {
        for (SagaState state : sagaStateRepository.findAllInProgress()) {
            long age = System.currentTimeMillis() - state.getUpdatedAt();
            if (age > timeoutMs) {
                log.warn("Saga {} timed out at step {} ({} ms), triggering compensation",
                        state.getSagaId(), state.getCurrentStep(), age);
                sagaOrchestrator.failAndRollback(state, "Timeout waiting for step " + state.getCurrentStep());
            }
        }
    }
}