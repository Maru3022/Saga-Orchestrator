package org.example.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.config.SagaTopicsProperties;
import org.example.model.SagaEvent;
import org.example.model.SagaResponseEvent;
import org.example.service.SagaOrchestrator;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class SagaResponseListener {

    private final SagaOrchestrator sagaOrchestrator;
    private final ObjectMapper objectMapper;
    private final SagaTopicsProperties topics;

    @KafkaListener(
            topics = {
                    "#{@sagaTopicsProperties.notificationResponseHyphen}",
                    "#{@sagaTopicsProperties.trainsResponse}",
                    "#{@sagaTopicsProperties.nutritionResponseHyphen}",
                    "#{@sagaTopicsProperties.userResponse}"
            },
            containerFactory = "sagaKafkaListenerContainerFactory",
            groupId = "saga-orchestrator-group"
    )
    public void onResponse(String rawMessage) {
        try {
            SagaResponseEvent response = objectMapper.readValue(rawMessage, SagaResponseEvent.class);
            log.info("Received saga response: sagaId={}, step={}, status={}",
                    response.getSagaId(), response.getStep(), response.getStatus());

            Map<String, Object> data = response.getData() != null
                    ? new HashMap<>(response.getData())
                    : new HashMap<>();

            SagaEvent sagaEvent = new SagaEvent(
                    response.getSagaId(),
                    response.getStep(),
                    mapStatus(response.getStatus()),
                    data
            );

            sagaOrchestrator.handleEvent(sagaEvent);

        } catch (Exception e) {
            log.error("Failed to process saga response: {}", rawMessage, e);
        }
    }

    private String mapStatus(String serviceStatus) {
        if ("SUCCESS".equals(serviceStatus)) return "SUCCESS";
        if ("ROLLBACK_DONE".equals(serviceStatus)) return "ROLLBACK";
        return "FAILED";
    }
}