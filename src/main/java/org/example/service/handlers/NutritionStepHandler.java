package org.example.service.handlers;

import lombok.extern.slf4j.Slf4j;
import org.example.model.SagaEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class NutritionStepHandler implements StepHandler {

    public static final String STEP = "NUTRITION";

    @Autowired
    private KafkaTemplate<String, SagaEvent> kafkaTemplate;

    @Override
    public void processForward(SagaEvent event){
        log.info("Forward NUTRITION step for saga {}", event.getSagaId());
        kafkaTemplate.send("saga-nutrition-command", event);
    }

    @Override
    public void processRollback(SagaEvent event){
        log.info("Rollback NUTRITION step for saga {}", event.getSagaId());
        SagaEvent rollbackEvent = new SagaEvent(event.getSagaId(), STEP, "ROLLBACK", event.getData());
        kafkaTemplate.send("saga-nutrition-command", rollbackEvent);
    }

    @Override
    public String getStepName(){
        return STEP;
    }
}