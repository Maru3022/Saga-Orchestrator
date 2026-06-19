package service;

import org.example.config.SagaTopicsProperties;
import org.example.model.SagaEvent;
import org.example.model.SagaState;
import org.example.repository.SagaStateRepository;
import org.example.service.SagaOrchestrator;
import org.example.service.handlers.StepHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SagaOrchestratorTest {

    private SagaStateRepository repository;
    private StepHandler stepHandler1;
    private StepHandler stepHandler2;
    private SagaOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        repository = mock(SagaStateRepository.class);
        stepHandler1 = mock(StepHandler.class);
        stepHandler2 = mock(StepHandler.class);
        var sagaTopicsProperties = mock(SagaTopicsProperties.class);

        when(stepHandler1.getStepName()).thenReturn("TRAINS");
        when(stepHandler2.getStepName()).thenReturn("NUTRITION");

        var sagaInstanceRepository = mock(org.example.repository.SagaInstanceRepository.class);
        var kafkaTemplate = mock(org.springframework.kafka.core.KafkaTemplate.class);
        var objectMapper = mock(com.fasterxml.jackson.databind.ObjectMapper.class);

        orchestrator = new SagaOrchestrator(sagaInstanceRepository, repository, kafkaTemplate, objectMapper,
            List.of(stepHandler1, stepHandler2), sagaTopicsProperties);
    }

    @Test
    void testStartSaga() {
        orchestrator.startSaga("saga-type", Map.of("key", "value"));
        verify(repository, atLeastOnce()).save(any(SagaState.class));
        verify(stepHandler1, times(1)).processForward(any(SagaEvent.class));
    }

    @Test
    void testHandleEvent_Success() {
        SagaState state = new SagaState("saga-type", Map.of("key", "value"));
        state.setSagaId("saga-1");
        state.setCurrentStep("TRAINS");
        state.setStatus(SagaState.STATUS_IN_PROGRESS);

        when(repository.findById("saga-1")).thenReturn(Optional.of(state));

        SagaEvent event = new SagaEvent("saga-1", "TRAINS", "SUCCESS", Map.of("key", "value"));
        orchestrator.handleEvent(event);

        verify(stepHandler2, times(1)).processForward(any(SagaEvent.class));
        verify(repository, atLeastOnce()).save(any(SagaState.class));
    }

    @Test
    void testHandleEvent_Rollback() {
        SagaState state = new SagaState("saga-type", Map.of("key", "value"));
        state.setSagaId("saga-1");
        state.setCurrentStep("NUTRITION");
        state.setStatus(SagaState.STATUS_IN_PROGRESS);

        when(repository.findById("saga-1")).thenReturn(Optional.of(state));

        SagaEvent event = new SagaEvent("saga-1", "NUTRITION", "FAILED", Map.of("key", "value"));
        orchestrator.handleEvent(event);

        verify(stepHandler2, times(1)).processRollback(any(SagaEvent.class));
        verify(stepHandler1, times(1)).processRollback(any(SagaEvent.class));
    }
}