package service.handlers;

import jakarta.inject.Inject;
import org.example.model.SagaEvent;
import org.example.service.handlers.NotificationStepHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class NotificationStepHandlerTest {

    @Mock
    private KafkaTemplate<String, SagaEvent> kafkaTemplate;

    @InjectMocks
    private NotificationStepHandler handler;

    @Test
    void shouldProcessForward(){
        SagaEvent sagaEvent = new SagaEvent("saga-1","NOTIFICATION","EXECUTE",null);
        handler.processForward(sagaEvent);
        verify(kafkaTemplate).send(eq("saga-notification-command"),any(SagaEvent.class));
    }

    @Test
    void shouldProcessRollBack(){
        SagaEvent event = new SagaEvent("saga-2","NOTIFICATION","ROLLBACK","data");
        handler.processRollback(event);
        verify(kafkaTemplate).send(eq("saga-notification-command"),any(SagaEvent.class));
    }

    @Test
    void shouldReturnStepName(){
        assertThat(handler.getStepName()).isEqualTo("NOTIFICATION");
    }

}
