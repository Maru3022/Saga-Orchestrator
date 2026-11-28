package org.example.service;

import org.example.repository.SagaStateRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
public class SagaOutboxCommittedListener {

    private final SagaStateRepository sagaStateRepository;

    public SagaOutboxCommittedListener(SagaStateRepository sagaStateRepository) {
        this.sagaStateRepository = sagaStateRepository;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void updateRedisCache(SagaOutboxCommittedEvent event) {
        sagaStateRepository.save(event.getSagaState());
    }
}
