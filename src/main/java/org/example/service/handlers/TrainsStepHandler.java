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
public class TrainsStepHandler implements StepHandler {

    public static final String STEP = "TRAINS";

    private final KafkaTemplate<String, String> sagaKafkaTemplate;
    private final ObjectMapper objectMapper;
    private final SagaTopicsProperties topics;

    @Override
    public void processForward(SagaEvent event) {
        log.info("Forward TRAINS step for saga {}", event.getSagaId());
        sendCommand(event, "EXECUTE", "EXECUTE");
    }

    @Override
    public void processRollback(SagaEvent event) {
        log.info("Rollback TRAINS step for saga {}", event.getSagaId());
        sendCommand(event, "ROLLBACK", "COMPENSATE");
    }

    private void sendCommand(SagaEvent event, String status, String operation) {
        String sagaId = event.getSagaId();
        try {
            String commandId = event.getCommandId();
            SagaCommandEvent cmd = new SagaCommandEvent(
                    commandId == null ? sagaId + ":" + STEP + ":" + status
                            : java.util.UUID.randomUUID().toString(),
                    sagaId, STEP, status, event.getData());
            if (commandId != null) {
                cmd.setCommandId(commandId);
                cmd.setOperation(operation);
                cmd.setEventVersion(event.getEventVersion());
            }
            String json = objectMapper.writeValueAsString(cmd);
            sagaKafkaTemplate.send(topics.getTrainsCommand(), sagaId, json)
                    .get(10, TimeUnit.SECONDS);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize TRAINS command for saga {}", sagaId, e);
            throw new IllegalStateException("Serialization failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing TRAINS command", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Failed to publish TRAINS command", e);
        }
    }

    @Override
    public String getStepName() { return STEP; }
}