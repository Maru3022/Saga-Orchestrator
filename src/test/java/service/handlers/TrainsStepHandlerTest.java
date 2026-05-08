package org.example.service.handlers;

import org.apache.commons.lang.ObjectUtils;
import org.example.model.SagaEvent;
import org.example.model.SagaState;
import org.example.service.handlers.TrainsStepHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class TrainsStepHandlerTest {

    @Mock
    private KafkaTemplate<String, SagaEvent> kafkaTemplate;

    @InjectMocks
    private TrainsStepHandler handler;

    @Test
    void shouldProcessForward(){
        SagaEvent event = new SagaEvent("saga-1","TRAINS","EXECUTE", null);
        handler.processForward(event);

        ArgumentCaptor<SagaEvent> captor = ArgumentCaptor.forClass(SagaEvent.class);
        verify(kafkaTemplate).send(eq("saga-trains-command"), captor.capture());
        assertThat(captor.getValue().getSagaId()).isEqualTo("saga-1");
    }


    @Test
    void shouldProcessRollback(){
        SagaEvent event = new SagaEvent("saga-2","TRAINS","ROLLBACK", "data");
        handler.processRollback(event);

        ArgumentCaptor<SagaEvent> captor = ArgumentCaptor.forClass(SagaEvent.class);
        verify(kafkaTemplate).send(eq("saga-trains-command"),captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo("ROLLBACK");
    }

    @Test
    void shouldReturnStepName(){
        assertThat(handler.getStepName()).isEqualTo("TRAINS");
    }
}
