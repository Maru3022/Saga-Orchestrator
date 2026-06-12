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
    public void processForward(SagaEvent event) {
        log.info("Processing forward NUTRITION step for saga {}", event.getSagaId());
        if (event.getData() == null || event.getData().isEmpty()) {
            log.error("Saga {} failed: missing data for NUTRITION step", event.getSagaId());
            return;
        }
        kafkaTemplate.send("saga-nutrition-command", event);
    }

    @Override
    public void processRollback(SagaEvent event) {
        log.info("Processing rollback NUTRITION step for saga {}", event.getSagaId());
        SagaEvent rollbackEvent = new SagaEvent(
                event.getSagaId(),
                STEP,
                "ROLLBACK",
                event.getData()
        );
        kafkaTemplate.send("saga-nutrition-command", rollbackEvent);
    }

    @Override
    public String getStepName() {
        return STEP;
    }
}