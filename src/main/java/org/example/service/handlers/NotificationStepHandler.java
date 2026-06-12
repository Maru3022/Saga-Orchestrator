package org.example.service.handlers;

import lombok.extern.slf4j.Slf4j;
import org.example.model.SagaEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class NotificationStepHandler implements StepHandler {

    public static final String STEP = "NOTIFICATION";

    @Autowired
    private KafkaTemplate<String, SagaEvent> kafkaTemplate;

    @Override
    public void processForward(SagaEvent event) {
        log.info("Forward NOTIFICATION step for saga {}", event.getSagaId());
        kafkaTemplate.send("saga-notification-command", event);
    }

    @Override
    public void processRollback(SagaEvent event) {
        log.info("Rollback NOTIFICATION step for saga {}", event.getSagaId());
        SagaEvent rollbackEvent = new SagaEvent(event.getSagaId(), STEP, "ROLLBACK", event.getData());
        kafkaTemplate.send("saga-notification-command", rollbackEvent);
    }

    @Override
    public String getStepName() {
        return STEP;
    }
}