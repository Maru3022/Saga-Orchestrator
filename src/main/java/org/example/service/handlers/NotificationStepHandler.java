package org.example.service.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.config.SagaTopicsProperties;
import org.example.model.SagaCommandEvent;
import org.example.model.SagaEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class NotificationStepHandler implements StepHandler {

    public static final String STEP = "NOTIFICATION";

    private final KafkaTemplate<String, String> sagaKafkaTemplate;
    private final ObjectMapper objectMapper;
    private final SagaTopicsProperties topics;

    @Override
    public void processForward(SagaEvent event) {
        log.info("Forward NOTIFICATION step for saga {}", event.getSagaId());
        sendCommand(event.getSagaId(), "EXECUTE", event.getData());
    }

    @Override
    public void processRollback(SagaEvent event) {
        log.info("Rollback NOTIFICATION step for saga {}", event.getSagaId());
        sendCommand(event.getSagaId(), "ROLLBACK", event.getData());
    }

    private void sendCommand(String sagaId, String status, Map<String, Object> data) {
        try {
            SagaCommandEvent cmd = new SagaCommandEvent(
                    UUID.randomUUID().toString(), sagaId, STEP, status, data);
            String json = objectMapper.writeValueAsString(cmd);
            sagaKafkaTemplate.send(topics.getNotificationCommand(), sagaId, json);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize NOTIFICATION command for saga {}", sagaId, e);
            throw new IllegalStateException("Serialization failed", e);
        }
    }

    @Override
    public String getStepName() { return STEP; }
}