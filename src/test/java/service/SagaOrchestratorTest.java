package org.example.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.example.model.SagaEvent;
import org.example.model.SagaState;
import org.example.repository.SagaStateRepository;
import org.example.service.SagaOrchestrator;
import org.example.service.handlers.NotificationStepHandler;
import org.example.service.handlers.NutritionStepHandler;
import org.example.service.handlers.StepHandler;
import org.example.service.handlers.TrainsStepHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
class SagaOrchestratorTest {

    @Mock
    private SagaStateRepository sagaStateRepository;

    @Mock
    private KafkaTemplate<String, SagaEvent> kafkaTemplate;

    @Spy
    private TrainsStepHandler trainsStepHandler = new TrainsStepHandler();

    @Spy
    private NutritionStepHandler nutritionStepHandler = new NutritionStepHandler();

    @Spy
    private NotificationStepHandler notificationStepHandler = new NotificationStepHandler();

    @InjectMocks
    private SagaOrchestrator orchestrator;

    private Map<String, StepHandler> stepHandlersMap;

    @BeforeEach
    void setUp() {
        trainsStepHandler = Mockito.spy(new TrainsStepHandler());
        ReflectionTestUtils.setField(trainsStepHandler, "kafkaTemplate", kafkaTemplate);

        nutritionStepHandler = Mockito.spy(new NutritionStepHandler());
        ReflectionTestUtils.setField(nutritionStepHandler, "kafkaTemplate", kafkaTemplate);

        notificationStepHandler = Mockito.spy(new NotificationStepHandler());
        ReflectionTestUtils.setField(notificationStepHandler, "kafkaTemplate", kafkaTemplate);

        stepHandlersMap = Map.of(
                "trainsStepHandler", trainsStepHandler,
                "nutritionStepHandler", nutritionStepHandler,
                "notificationStepHandler", notificationStepHandler
        );

        orchestrator = new SagaOrchestrator();
        ReflectionTestUtils.setField(orchestrator, "stepHandlers", stepHandlersMap);
        ReflectionTestUtils.setField(orchestrator, "sagaStateRepository", sagaStateRepository);
        ReflectionTestUtils.setField(orchestrator, "kafkaTemplate", kafkaTemplate);
    }

    @Test
    void shouldStartSagaAndProceedToFirstStep() {
        Map<String, Object> payload = Map.of("userId", "123");
        when(sagaStateRepository.save(any())).thenReturn(null);

        SagaState state = orchestrator.startSaga("CREATE_PROGRAM", payload);

        assertThat(state.getSagaType()).isEqualTo("CREATE_PROGRAM");
        assertThat(state.getCurrentStep()).isEqualTo("TRAINS");
        verify(sagaStateRepository, atLeast(2)).save(any());
        verify(trainsStepHandler).processForward(any(SagaEvent.class));
    }

    @Test
    void shouldCompleteSagaWhenAllStepsSucceed() {
        Map<String, Object> payload = new HashMap<>();
        SagaState state = new SagaState("CREATE_PROGRAM", payload);
        state.setSagaId("saga-1");
        state.setCurrentStep("NOTIFICATION");

        when(sagaStateRepository.findById("saga-1")).thenReturn(Optional.of(state));

        SagaEvent response = new SagaEvent("saga-1", "NOTIFICATION", "SUCCESS", null);
        orchestrator.onServiceResponse(response);

        assertThat(state.getCurrentStep()).isEqualTo("COMPLETED");
        verify(sagaStateRepository).save(state);
    }

    @Test
    void shouldRollbackOnStepFailure() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", "123");
        SagaState state = new SagaState("CREATE_PROGRAM", payload);
        state.setSagaId("saga-2");
        state.setCurrentStep("NUTRITION");

        when(sagaStateRepository.findById("saga-2")).thenReturn(Optional.of(state));

        SagaEvent response = new SagaEvent("saga-2", "NUTRITION", "FAILURE", "DB error");
        orchestrator.onServiceResponse(response);

        assertThat(state.getCurrentStep()).isEqualTo("FAILED");
        assertThat(state.getFailureReason()).isEqualTo("DB error");

        verify(trainsStepHandler).processRollback(any(SagaEvent.class));
        verify(nutritionStepHandler, never()).processRollback(any());
        verify(notificationStepHandler, never()).processRollback(any());
        verify(sagaStateRepository).delete("saga-2");
    }

    @Test
    void shouldThrowExceptionForUnknownSagaType() {
        assertThatThrownBy(() -> orchestrator.startSaga("UNKNOWN", Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown saga type");
    }

    @Test
    void shouldHandleMissingStateGracefully() {
        when(sagaStateRepository.findById("missing")).thenReturn(Optional.empty());
        SagaEvent event = new SagaEvent("missing", "TRAINS", "SUCCESS", null);
        assertThatCode(() -> orchestrator.onServiceResponse(event)).doesNotThrowAnyException();
    }
}