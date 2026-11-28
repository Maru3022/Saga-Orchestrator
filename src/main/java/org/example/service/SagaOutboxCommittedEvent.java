package org.example.service;

import org.example.model.SagaState;

public class SagaOutboxCommittedEvent {
    private final SagaState sagaState;

    public SagaOutboxCommittedEvent(SagaState sagaState) {
        this.sagaState = sagaState;
    }

    public SagaState getSagaState() {
        return sagaState;
    }
}
