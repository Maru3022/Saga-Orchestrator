package org.example.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.config.SagaTopicsProperties;
import org.example.model.NotificationResponseEvent;
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
public class NotificationResponseListener {

    private final SagaOrchestrator sagaOrchestrator;
    private final ObjectMapper objectMapper;
    private final SagaTopicsProperties topics;

    @KafkaListener(
            topics = "#{@sagaTopicsProperties.notificationResponse}",
            containerFactory = "sagaKafkaListenerContainerFactory",
            groupId = "saga-orchestrator-group"
    )
    public void onNotificationResponse(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {
        try {
            NotificationResponseEvent event = objectMapper.readValue(payload, NotificationResponseEvent.class);
            String correlationId = event.getCorrelationId();
            
            log.info("Notification response received - correlationId={}, success={}, userId={}, partition={}, offset={}",
                    correlationId, event.isSuccess(), event.getUserId(), partition, offset);

            sagaOrchestrator.handleNotificationResponse(event);
            
            ack.acknowledge();
            
            log.info("Notification response processed successfully - correlationId={}", correlationId);

        } catch (Exception e) {
            log.error("Failed to process notification response event: {}", payload, e);
        }
    }
}
