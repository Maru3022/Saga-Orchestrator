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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
@RequiredArgsConstructor
public class NutritionStepHandler implements StepHandler {

    public static final String STEP = "NUTRITION";

    private final KafkaTemplate<String, String> sagaKafkaTemplate;
    private final ObjectMapper objectMapper;
    private final SagaTopicsProperties topics;

    @Override
    public void processForward(SagaEvent event) {
        log.info("Forward NUTRITION step for saga {}", event.getSagaId());
        sendCommand(event.getSagaId(), "EXECUTE", event.getData());
    }

    @Override
    public void processRollback(SagaEvent event) {
        log.info("Rollback NUTRITION step for saga {}", event.getSagaId());
        sendCommand(event.getSagaId(), "ROLLBACK", event.getData());
    }

    private void sendCommand(String sagaId, String status, Map<String, Object> data) {
        try {
            SagaCommandEvent cmd = new SagaCommandEvent(
                    sagaId + ":" + STEP + ":" + status, sagaId, STEP, status, data);
            String json = objectMapper.writeValueAsString(cmd);
            sagaKafkaTemplate.send(topics.getNutritionCommand(), sagaId, json)
                    .get(10, TimeUnit.SECONDS);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize NUTRITION command for saga {}", sagaId, e);
            throw new IllegalStateException("Serialization failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing NUTRITION command", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Failed to publish NUTRITION command", e);
        }
    }

    @Override
    public String getStepName() { return STEP; }
}