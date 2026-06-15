package org.example.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.model.UserCreatedEvent;
import org.example.service.SagaOrchestrator;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class UserCreatedListener {

    private final SagaOrchestrator sagaOrchestrator;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "saga.user.created",
            containerFactory = "sagaKafkaListenerContainerFactory",
            groupId = "saga-orchestrator-group"
    )
    public void onUserCreated(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {
        try {
            UserCreatedEvent event = objectMapper.readValue(payload, UserCreatedEvent.class);
            String correlationId = event.getCorrelationId();
            
            log.info("User created event received - correlationId={}, userId={}, email={}, partition={}, offset={}",
                    correlationId, event.getUserId(), event.getEmail(), partition, offset);

            sagaOrchestrator.startSaga(event);
            
            ack.acknowledge();
            
            log.info("User created event processed successfully - correlationId={}", correlationId);

        } catch (Exception e) {
            log.error("Failed to process user.created event: {}", payload, e);
        }
    }
}
