package org.example.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.model.NutritionResponseEvent;
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
public class NutritionResponseListener {

    private final SagaOrchestrator sagaOrchestrator;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "saga.nutrition.response",
            containerFactory = "sagaKafkaListenerContainerFactory",
            groupId = "saga-orchestrator-group"
    )
    public void onNutritionResponse(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {
        try {
            NutritionResponseEvent event = objectMapper.readValue(payload, NutritionResponseEvent.class);
            String correlationId = event.getCorrelationId();
            
            log.info("Nutrition response received - correlationId={}, success={}, userId={}, partition={}, offset={}",
                    correlationId, event.isSuccess(), event.getUserId(), partition, offset);

            sagaOrchestrator.handleNutritionResponse(event);
            
            ack.acknowledge();
            
            log.info("Nutrition response processed successfully - correlationId={}", correlationId);

        } catch (Exception e) {
            log.error("Failed to process nutrition response event: {}", payload, e);
        }
    }
}
