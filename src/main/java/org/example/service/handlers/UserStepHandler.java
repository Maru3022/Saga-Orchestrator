package org.example.service.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.model.SagaCommandEvent;
import org.example.model.SagaEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class UserStepHandler implements StepHandler {

    public static final String STEP = "USER";

    private final KafkaTemplate<String, String> sagaKafkaTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public void processForward(SagaEvent event) {
        log.debug("USER step forward is no-op for saga {}", event.getSagaId());
    }

    @Override
    public void processRollback(SagaEvent event) {
        log.info("Compensating USER step (soft-delete) for saga {}", event.getSagaId());
        try {
            SagaCommandEvent cmd = new SagaCommandEvent(
                    UUID.randomUUID().toString(), event.getSagaId(), STEP, "ROLLBACK", event.getData());
            String json = objectMapper.writeValueAsString(cmd);
            sagaKafkaTemplate.send("saga-user-command", event.getSagaId(), json);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize USER rollback for saga {}", event.getSagaId(), e);
        }
    }

    @Override
    public String getStepName() { return STEP; }
}