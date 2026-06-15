package org.example.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.model.UserCreatedEvent;
import org.example.service.SagaOrchestrator;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class UserCreatedListener {

    private final SagaOrchestrator sagaOrchestrator;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "user.created",
            containerFactory = "sagaKafkaListenerContainerFactory",
            groupId = "saga-orchestrator-group"
    )
    public void onUserCreated(String rawMessage) {
        try {
            UserCreatedEvent event = objectMapper.readValue(rawMessage, UserCreatedEvent.class);
            log.info("User created event received: userId={}, email={}", event.getUserId(), event.getEmail());

            Map<String, Object> payload = new HashMap<>();
            payload.put("userId", event.getUserId());
            payload.put("username", event.getUsername());
            payload.put("email", event.getEmail());
            payload.put("fullName", event.getFullName());

            sagaOrchestrator.startSaga(SagaOrchestrator.SAGA_TYPE, payload);

        } catch (Exception e) {
            log.error("Failed to process user.created event: {}", rawMessage, e);
        }
    }
}