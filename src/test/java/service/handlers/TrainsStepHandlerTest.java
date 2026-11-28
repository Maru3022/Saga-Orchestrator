package service.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.config.SagaTopicsProperties;
import org.example.model.SagaCommandEvent;
import org.example.model.SagaEvent;
import org.example.service.handlers.TrainsStepHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TrainsStepHandlerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private SagaTopicsProperties sagaTopicsProperties;

    @InjectMocks
    private TrainsStepHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(sagaTopicsProperties.getTrainsCommand()).thenReturn("saga-trains-command");
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void testGetStepName() {
        assertEquals("TRAINS", handler.getStepName());
    }

    @Test
    void testProcessForward() {
        SagaEvent event = new SagaEvent("saga-1", "TRAINS", "START", Map.of("data", "value"));
        handler.processForward(event);
        verify(kafkaTemplate, times(1)).send(eq("saga-trains-command"), eq("saga-1"), anyString());
    }

    @Test
    void testProcessRollback() {
        SagaEvent event = new SagaEvent("saga-1", "TRAINS", "START", Map.of("data", "value"));
        handler.processRollback(event);
        verify(kafkaTemplate, times(1)).send(eq("saga-trains-command"), eq("saga-1"), anyString());
    }

    @Test
    void testBrokerSendFailureIsReportedToActivity() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(java.util.concurrent.CompletableFuture.failedFuture(
                        new IllegalStateException("broker unavailable")));

        assertThrows(IllegalStateException.class,
                () -> handler.processForward(new SagaEvent(
                        "saga-1", "TRAINS", "EXECUTE", Map.of("data", "value"))));
    }

    @Test
    void temporalCommandCarriesStableCommandIdAndCompensationOperation() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        when(objectMapper.writeValueAsString(any())).thenAnswer(invocation ->
                mapper.writeValueAsString(invocation.getArgument(0)));
        SagaEvent event = new SagaEvent("temporal-operation-1", "TRAINING", "FAILED",
                Map.of("userId", "owner-1"));
        event.setCommandId("temporal-operation-1:TRAINS:ROLLBACK");
        event.setOperation("COMPENSATE");
        event.setEventVersion(1);

        handler.processRollback(event);

        var payload = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("saga-trains-command"), eq("temporal-operation-1"), payload.capture());
        SagaCommandEvent command = mapper.readValue(payload.getValue(), SagaCommandEvent.class);
        assertEquals("temporal-operation-1:TRAINS:ROLLBACK", command.getCommandId());
        assertEquals("COMPENSATE", command.getOperation());
        assertEquals(1, command.getEventVersion());
        assertEquals("ROLLBACK", command.getStatus());
    }
}