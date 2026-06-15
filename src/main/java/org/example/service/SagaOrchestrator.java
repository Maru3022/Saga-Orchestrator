package org.example.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.model.*;
import org.example.repository.SagaInstanceRepository;
import org.example.repository.SagaStateRepository;
import org.example.service.handlers.StepHandler;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrator {

    public static final String SAGA_TYPE = "CREATE_PROGRAM";

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaStateRepository sagaStateRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final List<StepHandler> stepHandlers;

    @Transactional
    public void startSaga(UserCreatedEvent event) {
        String correlationId = event.getCorrelationId();
        
        log.info("Starting saga - correlationId={}, userId={}, email={}",
                correlationId, event.getUserId(), event.getEmail());

        // Idempotency check
        if (sagaInstanceRepository.existsByCorrelationId(correlationId)) {
            log.warn("Saga already exists for correlationId={}, ignoring duplicate", correlationId);
            return;
        }

        // Create SagaInstance
        SagaInstance sagaInstance = SagaInstance.builder()
                .id(UUID.randomUUID())
                .correlationId(correlationId)
                .userId(event.getUserId())
                .userEmail(event.getEmail())
                .state(SagaStatus.STARTED)
                .currentStep(SagaStep.SEND_NOTIFICATION)
                .retryCount(0)
                .maxRetries(3)
                .build();

        try {
            String payload = objectMapper.writeValueAsString(event);
            sagaInstance.setSagaPayload(payload);
        } catch (Exception e) {
            log.error("Failed to serialize saga payload - correlationId={}", correlationId, e);
            sagaInstance.setErrorMessage("Failed to serialize payload: " + e.getMessage());
        }

        sagaInstance = sagaInstanceRepository.save(sagaInstance);
        
        log.info("Saga instance created - correlationId={}, sagaId={}, state={}",
                correlationId, sagaInstance.getId(), sagaInstance.getState());

        // Send to notification service
        sendNotificationRequest(sagaInstance, event);
    }

    @Transactional
    public SagaState startSaga(String sagaType, Map<String, Object> payload) {
        SagaState state = new SagaState(sagaType, payload);
        if (!stepHandlers.isEmpty()) {
            String firstStep = stepHandlers.get(0).getStepName();
            state.setCurrentStep(firstStep);
        }
        sagaStateRepository.save(state);

        if (!stepHandlers.isEmpty()) {
            StepHandler firstHandler = stepHandlers.get(0);
            firstHandler.processForward(new SagaEvent(state.getSagaId(), state.getCurrentStep(), "START", payload));
        }

        return state;
    }

    public void handleEvent(SagaEvent sagaEvent) {
        SagaState state = sagaStateRepository.findById(sagaEvent.getSagaId()).orElse(null);
        if (state == null) {
            log.error("Saga state not found - sagaId={}", sagaEvent.getSagaId());
            return;
        }

        if (!SagaState.STATUS_IN_PROGRESS.equals(state.getStatus())) {
            log.warn("Ignoring saga event for non-active state - sagaId={}, status={}",
                    sagaEvent.getSagaId(), state.getStatus());
            return;
        }

        if ("SUCCESS".equals(sagaEvent.getStatus())) {
            findNextHandler(sagaEvent.getStepName())
                    .ifPresent(handler -> {
                        handler.processForward(sagaEvent);
                        state.setCurrentStep(handler.getStepName());
                        sagaStateRepository.save(state);
                    });
        } else if ("FAILED".equals(sagaEvent.getStatus())) {
            rollbackFailedStep(sagaEvent.getStepName(), sagaEvent);
        } else {
            log.warn("Unhandled saga event status - sagaId={}, status={}",
                    sagaEvent.getSagaId(), sagaEvent.getStatus());
        }
    }

    private Optional<StepHandler> findNextHandler(String currentStep) {
        for (int i = 0; i < stepHandlers.size(); i++) {
            if (stepHandlers.get(i).getStepName().equals(currentStep) && i + 1 < stepHandlers.size()) {
                return Optional.of(stepHandlers.get(i + 1));
            }
        }
        return Optional.empty();
    }

    private void rollbackFailedStep(String failedStep, SagaEvent sagaEvent) {
        int failedIndex = -1;
        for (int i = 0; i < stepHandlers.size(); i++) {
            if (stepHandlers.get(i).getStepName().equals(failedStep)) {
                failedIndex = i;
                break;
            }
        }

        if (failedIndex == -1) {
            log.warn("No handler found for failed step - sagaId={}, step={}", sagaEvent.getSagaId(), failedStep);
            return;
        }

        for (int i = failedIndex; i >= 0; i--) {
            stepHandlers.get(i).processRollback(sagaEvent);
        }
    }

    public void failAndRollback(SagaState state, String reason) {
        if (state == null) {
            return;
        }
        log.warn("Failing saga state {} due to timeout: {}", state.getSagaId(), reason);
        state.setStatus(SagaState.STATUS_FAILED);
        state.setFailureReason(reason);
        sagaStateRepository.save(state);
    }

    private void sendNotificationRequest(SagaInstance sagaInstance, UserCreatedEvent event) {
        try {
            String correlationId = sagaInstance.getCorrelationId();
            String topic = "saga.notification.send";
            
            String message = objectMapper.writeValueAsString(event);
            
            kafkaTemplate.send(topic, correlationId, message);
            
            log.info("Sent notification request - correlationId={}, topic={}, step={}",
                    correlationId, topic, SagaStep.SEND_NOTIFICATION);

            sagaInstance.setState(SagaStatus.USER_CREATED);
            sagaInstanceRepository.save(sagaInstance);
        } catch (Exception e) {
            log.error("Failed to send notification request - correlationId={}",
                    sagaInstance.getCorrelationId(), e);
            handleFailure(sagaInstance, "Failed to send notification: " + e.getMessage());
        }
    }

    @Transactional
    public void handleNotificationResponse(NotificationResponseEvent event) {
        String correlationId = event.getCorrelationId();
        
        log.info("Handling notification response - correlationId={}, success={}, userId={}",
                correlationId, event.isSuccess(), event.getUserId());

        SagaInstance sagaInstance = sagaInstanceRepository.findByCorrelationId(correlationId)
                .orElse(null);

        if (sagaInstance == null) {
            log.error("Saga instance not found - correlationId={}", correlationId);
            return;
        }

        if (event.isSuccess()) {
            sagaInstance.setState(SagaStatus.NOTIFICATION_SENT);
            sagaInstance.setCurrentStep(SagaStep.CREATE_CABINET);
            sagaInstanceRepository.save(sagaInstance);
            
            log.info("Notification sent successfully - correlationId={}, moving to next step",
                    correlationId);
            
            sendCabinetRequest(sagaInstance);
        } else {
            log.warn("Notification failed - correlationId={}, error={}", 
                    correlationId, event.getErrorMessage());
            sagaInstance.setState(SagaStatus.NOTIFICATION_FAILED);
            sagaInstanceRepository.save(sagaInstance);
            handleFailure(sagaInstance, event.getErrorMessage());
        }
    }

    private void sendCabinetRequest(SagaInstance sagaInstance) {
        try {
            String correlationId = sagaInstance.getCorrelationId();
            String topic = "saga.cabinet.create";
            
            UserCreatedEvent event = objectMapper.readValue(sagaInstance.getSagaPayload(), 
                    UserCreatedEvent.class);
            
            String message = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(topic, correlationId, message);
            
            log.info("Sent cabinet create request - correlationId={}, topic={}, step={}",
                    correlationId, topic, SagaStep.CREATE_CABINET);
        } catch (Exception e) {
            log.error("Failed to send cabinet request - correlationId={}",
                    sagaInstance.getCorrelationId(), e);
            handleFailure(sagaInstance, "Failed to send cabinet request: " + e.getMessage());
        }
    }

    @Transactional
    public void handleCabinetResponse(CabinetResponseEvent event) {
        String correlationId = event.getCorrelationId();
        
        log.info("Handling cabinet response - correlationId={}, success={}, userId={}",
                correlationId, event.isSuccess(), event.getUserId());

        SagaInstance sagaInstance = sagaInstanceRepository.findByCorrelationId(correlationId)
                .orElse(null);

        if (sagaInstance == null) {
            log.error("Saga instance not found - correlationId={}", correlationId);
            return;
        }

        if (event.isSuccess()) {
            sagaInstance.setState(SagaStatus.CABINET_CREATED);
            sagaInstance.setCurrentStep(SagaStep.CALCULATE_NUTRITION);
            sagaInstanceRepository.save(sagaInstance);
            
            log.info("Cabinet created successfully - correlationId={}, cabinetId={}, moving to next step",
                    correlationId, event.getCabinetId());
            
            sendNutritionRequest(sagaInstance);
        } else {
            log.warn("Cabinet creation failed - correlationId={}, error={}",
                    correlationId, event.getErrorMessage());
            sagaInstance.setState(SagaStatus.CABINET_FAILED);
            sagaInstanceRepository.save(sagaInstance);
            handleFailure(sagaInstance, event.getErrorMessage());
        }
    }

    private void sendNutritionRequest(SagaInstance sagaInstance) {
        try {
            String correlationId = sagaInstance.getCorrelationId();
            String topic = "saga.nutrition.calculate";
            
            UserCreatedEvent event = objectMapper.readValue(sagaInstance.getSagaPayload(),
                    UserCreatedEvent.class);
            
            String message = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(topic, correlationId, message);
            
            log.info("Sent nutrition calculate request - correlationId={}, topic={}, step={}",
                    correlationId, topic, SagaStep.CALCULATE_NUTRITION);
        } catch (Exception e) {
            log.error("Failed to send nutrition request - correlationId={}",
                    sagaInstance.getCorrelationId(), e);
            handleFailure(sagaInstance, "Failed to send nutrition request: " + e.getMessage());
        }
    }

    @Transactional
    public void handleNutritionResponse(NutritionResponseEvent event) {
        String correlationId = event.getCorrelationId();
        
        log.info("Handling nutrition response - correlationId={}, success={}, userId={}",
                correlationId, event.isSuccess(), event.getUserId());

        SagaInstance sagaInstance = sagaInstanceRepository.findByCorrelationId(correlationId)
                .orElse(null);

        if (sagaInstance == null) {
            log.error("Saga instance not found - correlationId={}", correlationId);
            return;
        }

        if (event.isSuccess()) {
            sagaInstance.setState(SagaStatus.COMPLETED);
            sagaInstance.setCompletedAt(Instant.now());
            sagaInstanceRepository.save(sagaInstance);
            
            log.info("Saga completed successfully - correlationId={}, completedAt={}",
                    correlationId, sagaInstance.getCompletedAt());
        } else {
            log.warn("Nutrition calculation failed - correlationId={}, error={}",
                    correlationId, event.getErrorMessage());
            sagaInstance.setState(SagaStatus.NUTRITION_FAILED);
            sagaInstanceRepository.save(sagaInstance);
            handleFailure(sagaInstance, event.getErrorMessage());
        }
    }

    @Transactional
    public void handleFailure(SagaInstance sagaInstance, String errorMessage) {
        String correlationId = sagaInstance.getCorrelationId();
        
        log.warn("Handling saga failure - correlationId={}, currentStep={}, retryCount={}, error={}",
                correlationId, sagaInstance.getCurrentStep(), sagaInstance.getRetryCount(), errorMessage);

        if (sagaInstance.getRetryCount() < sagaInstance.getMaxRetries()) {
            sagaInstance.setRetryCount(sagaInstance.getRetryCount() + 1);
            sagaInstance.setErrorMessage(errorMessage);
            sagaInstanceRepository.save(sagaInstance);
            
            log.info("Retrying saga - correlationId={}, retryCount={}/{}, step={}",
                    correlationId, sagaInstance.getRetryCount(), sagaInstance.getMaxRetries(),
                    sagaInstance.getCurrentStep());
            
            retryCurrentStep(sagaInstance);
        } else {
            log.error("Max retries exceeded - correlationId={}, starting compensation, error={}",
                    correlationId, errorMessage);
            
            sagaInstance.setErrorMessage(errorMessage);
            startCompensation(sagaInstance);
        }
    }

    private void retryCurrentStep(SagaInstance sagaInstance) {
        try {
            String correlationId = sagaInstance.getCorrelationId();
            UserCreatedEvent event = objectMapper.readValue(sagaInstance.getSagaPayload(),
                    UserCreatedEvent.class);
            
            switch (sagaInstance.getCurrentStep()) {
                case SEND_NOTIFICATION:
                    sendNotificationRequest(sagaInstance, event);
                    break;
                case CREATE_CABINET:
                    sendCabinetRequest(sagaInstance);
                    break;
                case CALCULATE_NUTRITION:
                    sendNutritionRequest(sagaInstance);
                    break;
                default:
                    log.warn("Unknown step for retry - correlationId={}, step={}",
                            correlationId, sagaInstance.getCurrentStep());
            }
        } catch (Exception e) {
            log.error("Error during retry - correlationId={}",
                    sagaInstance.getCorrelationId(), e);
            startCompensation(sagaInstance);
        }
    }

    @Transactional
    public void startCompensation(SagaInstance sagaInstance) {
        String correlationId = sagaInstance.getCorrelationId();
        SagaStep failedStep = sagaInstance.getCurrentStep();
        
        log.info("Starting compensation - correlationId={}, failedStep={}, error={}",
                correlationId, failedStep, sagaInstance.getErrorMessage());

        sagaInstance.setState(SagaStatus.COMPENSATING);
        sagaInstanceRepository.save(sagaInstance);

        try {
            // Send compensation in reverse order
            switch (failedStep) {
                case CALCULATE_NUTRITION:
                    // Failed at NUTRITION, compensate cabinet and notification
                    sendCabinetCompensation(sagaInstance);
                    break;
                case CREATE_CABINET:
                    // Failed at CABINET, compensate notification
                    sendNotificationCompensation(sagaInstance);
                    break;
                case SEND_NOTIFICATION:
                case CREATE_USER:
                default:
                    // Failed at NOTIFICATION or USER, skip to user compensation
                    break;
            }
            
            // Always send user compensation as last step
            sendUserCompensation(sagaInstance);
        } catch (Exception e) {
            log.error("Error during compensation - correlationId={}", correlationId, e);
            sagaInstance.setState(SagaStatus.FAILED);
            sagaInstance.setErrorMessage("Compensation failed: " + e.getMessage());
            sagaInstanceRepository.save(sagaInstance);
        }
    }

    private void sendCabinetCompensation(SagaInstance sagaInstance) {
        try {
            String correlationId = sagaInstance.getCorrelationId();
            CompensationEvent event = CompensationEvent.builder()
                    .correlationId(correlationId)
                    .userId(sagaInstance.getUserId())
                    .action("DELETE_CABINET")
                    .reason(sagaInstance.getErrorMessage())
                    .build();

            String message = objectMapper.writeValueAsString(event);
            kafkaTemplate.send("saga.cabinet.compensate", correlationId, message);
            
            log.info("Sent cabinet compensation - correlationId={}, action=DELETE_CABINET",
                    correlationId);
        } catch (Exception e) {
            log.error("Failed to send cabinet compensation - correlationId={}",
                    sagaInstance.getCorrelationId(), e);
            throw new RuntimeException(e);
        }
    }

    private void sendNotificationCompensation(SagaInstance sagaInstance) {
        try {
            String correlationId = sagaInstance.getCorrelationId();
            CompensationEvent event = CompensationEvent.builder()
                    .correlationId(correlationId)
                    .userId(sagaInstance.getUserId())
                    .action("CANCEL_NOTIFICATION")
                    .reason(sagaInstance.getErrorMessage())
                    .build();

            String message = objectMapper.writeValueAsString(event);
            kafkaTemplate.send("saga.notification.compensate", correlationId, message);
            
            log.info("Sent notification compensation - correlationId={}, action=CANCEL_NOTIFICATION",
                    correlationId);
        } catch (Exception e) {
            log.error("Failed to send notification compensation - correlationId={}",
                    sagaInstance.getCorrelationId(), e);
            throw new RuntimeException(e);
        }
    }

    private void sendUserCompensation(SagaInstance sagaInstance) {
        try {
            String correlationId = sagaInstance.getCorrelationId();
            CompensationEvent event = CompensationEvent.builder()
                    .correlationId(correlationId)
                    .userId(sagaInstance.getUserId())
                    .action("DELETE_USER")
                    .reason(sagaInstance.getErrorMessage())
                    .build();

            String message = objectMapper.writeValueAsString(event);
            kafkaTemplate.send("saga.user.compensate", correlationId, message);
            
            log.info("Sent user compensation - correlationId={}, action=DELETE_USER",
                    correlationId);

            // Mark saga as compensated
            sagaInstance.setState(SagaStatus.COMPENSATED);
            sagaInstanceRepository.save(sagaInstance);
            
            log.info("Compensation completed - correlationId={}, state=COMPENSATED",
                    correlationId);
        } catch (Exception e) {
            log.error("Failed to send user compensation - correlationId={}",
                    sagaInstance.getCorrelationId(), e);
            sagaInstance.setState(SagaStatus.FAILED);
            sagaInstanceRepository.save(sagaInstance);
            throw new RuntimeException(e);
        }
    }
}
