package org.example.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.model.CabinetResponseEvent;
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
public class CabinetResponseListener {

    private final SagaOrchestrator sagaOrchestrator;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "saga.cabinet.response",
            containerFactory = "sagaKafkaListenerContainerFactory",
            groupId = "saga-orchestrator-group"
    )
    public void onCabinetResponse(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {
        try {
            CabinetResponseEvent event = objectMapper.readValue(payload, CabinetResponseEvent.class);
            String correlationId = event.getCorrelationId();
            
            log.info("Cabinet response received - correlationId={}, success={}, userId={}, cabinetId={}, partition={}, offset={}",
                    correlationId, event.isSuccess(), event.getUserId(), event.getCabinetId(), partition, offset);

            sagaOrchestrator.handleCabinetResponse(event);
            
            ack.acknowledge();
            
            log.info("Cabinet response processed successfully - correlationId={}", correlationId);

        } catch (Exception e) {
            log.error("Failed to process cabinet response event: {}", payload, e);
        }
    }
}
