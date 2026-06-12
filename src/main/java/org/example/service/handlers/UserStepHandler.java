package org.example.service.handlers;

import lombok.extern.slf4j.Slf4j;
import org.example.model.SagaEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class UserStepHandler implements StepHandler {

    public static final String STEP = "USER";

    @Autowired
    private KafkaTemplate<String, SagaEvent> kafkaTemplate;

    @Override
    public void processForward(SagaEvent event) {
        log.debug("USER step has no forward action for saga {}", event.getSagaId());
    }

    @Override
    public void processRollback(SagaEvent event) {
        log.info("Compensating USER (soft-delete) for saga {}", event.getSagaId());
        SagaEvent rollbackEvent = new SagaEvent(event.getSagaId(), STEP, "ROLLBACK", event.getData());
        kafkaTemplate.send("saga-user-command", rollbackEvent);
    }

    @Override
    public String getStepName() {
        return STEP;
    }
}