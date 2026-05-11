package org.example.service.handlers;

import org.example.model.SagaEvent;
import org.example.service.handlers.NutritionStepHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class NutritionStepHandlerTest {

    @Mock
    private KafkaTemplate<String, SagaEvent> kafkaTemplate;

    @InjectMocks
    private NutritionStepHandler nutritionStepHandler;

    @Test
    void shouldProcessForward(){
        SagaEvent sagaEvent = new SagaEvent("saga-1", "NUTRITION", "EXECUTE",null);
        nutritionStepHandler.processForward(sagaEvent);
        verify(kafkaTemplate).send(eq("saga--nutrition-command"),any(SagaEvent.class));
    }

    @Test
    void shouldProcessRollback(){
        SagaEvent event = new SagaEvent("saga-2","NUTRITION", "ROLLBACK", "data");
        nutritionStepHandler.processRollback(event);
        ArgumentCaptor<SagaEvent> captor = ArgumentCaptor.forClass(SagaEvent.class);
        verify(kafkaTemplate).send(eq("saga--nutrition-command"),captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo("ROLLBACK");
    }

    @Test
    void shouldReturnStepName(){
        assertThat(nutritionStepHandler.getStepName()).isEqualTo("NUTRITION");
    }

}
