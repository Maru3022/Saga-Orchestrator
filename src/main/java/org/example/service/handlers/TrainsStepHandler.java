package org.example.service.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.model.SagaCommandEvent;
import org.example.model.SagaEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class TrainsStepHandler implements StepHandler {

    public static final String STEP = "TRAINS";

    private final KafkaTemplate<String, String> sagaKafkaTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public void processForward(SagaEvent event) {
        log.info("Forward TRAINS step for saga {}", event.getSagaId());
        sendCommand(event.getSagaId(), "EXECUTE", event.getData());
    }

    @Override
    public void processRollback(SagaEvent event) {
        log.info("Rollback TRAINS step for saga {}", event.getSagaId());
        sendCommand(event.getSagaId(), "ROLLBACK", event.getData());
    }

    private void sendCommand(String sagaId, String status, Map<String, Object> data) {
        try {
            SagaCommandEvent cmd = new SagaCommandEvent(
                    UUID.randomUUID().toString(), sagaId, STEP, status, data);
            String json = objectMapper.writeValueAsString(cmd);
            sagaKafkaTemplate.send("saga-trains-command", sagaId, json);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize TRAINS command for saga {}", sagaId, e);
            throw new IllegalStateException("Serialization failed", e);
        }
    }

    @Override
    public String getStepName() { return STEP; }
}