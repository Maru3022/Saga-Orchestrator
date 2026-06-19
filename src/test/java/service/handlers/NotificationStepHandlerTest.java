package service.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.config.SagaTopicsProperties;
import org.example.model.SagaEvent;
import org.example.service.handlers.NotificationStepHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NotificationStepHandlerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private SagaTopicsProperties sagaTopicsProperties;

    @InjectMocks
    private NotificationStepHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(sagaTopicsProperties.getNotificationCommand()).thenReturn("saga-notification-command");
    }

    @Test
    void testGetStepName() {
        assertEquals("NOTIFICATION", handler.getStepName());
    }

    @Test
    void testProcessForward() {
        SagaEvent event = new SagaEvent("saga-1", "NOTIFICATION", "START", Map.of("key", "value"));
        handler.processForward(event);
        verify(kafkaTemplate, times(1)).send(eq("saga-notification-command"), eq("saga-1"), anyString());
    }

    @Test
    void testProcessRollback() {
        SagaEvent event = new SagaEvent("saga-1", "NOTIFICATION", "START", Map.of("key", "value"));
        handler.processRollback(event);
        verify(kafkaTemplate, times(1)).send(eq("saga-notification-command"), eq("saga-1"), anyString());
    }
}