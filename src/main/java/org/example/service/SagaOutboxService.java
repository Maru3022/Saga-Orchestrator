package org.example.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.model.OutboxEvent;
import org.example.model.SagaState;
import org.example.model.SagaInstance;
import org.example.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.annotation.Transactional;

@Service
public class SagaOutboxService {

    private final OutboxEventRepository repository;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;

    public SagaOutboxService(OutboxEventRepository repository,
                             ObjectMapper objectMapper,
                             ApplicationEventPublisher eventPublisher) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.eventPublisher = eventPublisher;
    }

    @Transactional
    public OutboxEvent create(String eventType, SagaState sagaState) {
        try {
            String payload = objectMapper.writeValueAsString(sagaState);
            OutboxEvent event = new OutboxEvent(
                    "SagaState",
                    sagaState.getSagaId(),
                    eventType,
                    payload);
            return repository.save(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize outbox event", e);
        }
    }

    @Transactional
    public OutboxEvent createForSagaInstance(String eventType,
                                             SagaInstance sagaInstance,
                                             org.example.model.SagaState sagaState) {
        try {
            String payload = objectMapper.writeValueAsString(sagaInstance);
            OutboxEvent event = new OutboxEvent(
                    "SagaInstance",
                    sagaInstance.getCorrelationId(),
                    eventType,
                    payload);
            OutboxEvent saved = repository.save(event);
            eventPublisher.publishEvent(new SagaOutboxCommittedEvent(sagaState));
            return saved;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize outbox event", e);
        }
    }
}
