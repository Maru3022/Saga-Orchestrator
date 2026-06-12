package org.example.service;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.example.model.SagaDefinition;
import org.example.model.SagaEvent;
import org.example.model.SagaState;
import org.example.model.UserCreatedEvent;
import org.example.repository.SagaStateRepository;
import org.example.service.handlers.StepHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class SagaOrchestrator {

    public static final String SAGA_TYPE = "CREATE_USER_PROFILE";

    private static final List<String> FORWARD_STEPS = List.of("NOTIFICATION", "TRAINS", "NUTRITION");

    private static final Duration DEDUPE_TTL = Duration.ofHours(24);

    @Autowired
    private Map<String, StepHandler> stepHandlers;
    @Autowired
    private SagaStateRepository sagaStateRepository;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private final Map<String, SagaDefinition> sagaDefinitions = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        sagaDefinitions.put(SAGA_TYPE, new SagaDefinition(SAGA_TYPE, FORWARD_STEPS, null));
    }

    @KafkaListener(topics = "user.created", containerFactory = "sagaKafkaListenerContainerFactory",
            groupId = "saga-orchestrator")
    public void onUserCreated(UserCreatedEvent event) {
        if (event == null || event.getUserId() == null) {
            log.warn("Received malformed user.created event: {}", event);
            return;
        }
        if (!tryMarkProcessed("user-created:" + event.getEventId())) {
            log.info("Duplicate user.created event {} ignored", event.getEventId());
            return;
        }

        Map<String, Object> payload = new HashMap<>();
        payload.put("userId", event.getUserId());
        payload.put("username", event.getUsername());
        payload.put("email", event.getEmail());
        payload.put("fullName", event.getFullName());

        startSaga(SAGA_TYPE, payload);
    }

    public SagaState startSaga(String sagaType, Map<String, Object> payload) {
        SagaDefinition definition = sagaDefinitions.get(sagaType);
        if (definition == null) {
            throw new IllegalArgumentException("Unknown saga type: " + sagaType);
        }
        SagaState state = new SagaState(sagaType, payload);
        sagaStateRepository.save(state);
        log.info("Saga {} started with id {} for userId={}", sagaType, state.getSagaId(), payload.get("userId"));

        proceedToNextStep(state);
        return state;
    }

    private void proceedToNextStep(SagaState state) {
        int nextIndex = state.getCompletedSteps().size();
        if (nextIndex >= FORWARD_STEPS.size()) {
            state.setCurrentStep("COMPLETED");
            state.setStatus(SagaState.STATUS_COMPLETED);
            sagaStateRepository.save(state);
            log.info("Saga {} completed successfully", state.getSagaId());
            return;
        }

        String nextStep = FORWARD_STEPS.get(nextIndex);
        state.setCurrentStep(nextStep);
        state.setStatus(SagaState.STATUS_IN_PROGRESS);
        sagaStateRepository.save(state);

        StepHandler handler = findHandler(nextStep);
        SagaEvent event = new SagaEvent(state.getSagaId(), nextStep, "EXECUTE", state.getPayload());
        handler.processForward(event);
    }

    @KafkaListener(topics = {
            "saga-trains-response",
            "saga-nutrition-response",
            "saga-notification-response",
            "saga-user-response"
    }, containerFactory = "sagaKafkaListenerContainerFactory", groupId = "saga-orchestrator")
    public void onServiceResponse(SagaEvent event) {
        log.info("Received response: sagaId={}, step={}, status={}, eventId={}",
                event.getSagaId(), event.getStep(), event.getStatus(), event.getEventId());

        if (!tryMarkProcessed("response:" + event.getSagaId() + ":" + event.getEventId())) {
            log.info("Duplicate response event {} ignored", event.getEventId());
            return;
        }

        Optional<SagaState> optState = sagaStateRepository.findById(event.getSagaId());
        if (optState.isEmpty()) {
            log.warn("Saga state not found for id {}", event.getSagaId());
            return;
        }
        SagaState state = optState.get();

        if ("ROLLBACK_DONE".equals(event.getStatus())) {
            handleRollbackDone(state, event);
            return;
        }

        if (!event.getStep().equals(state.getCurrentStep())) {
            log.warn("Ignoring response for step {} - saga {} is currently at step {}",
                    event.getStep(), state.getSagaId(), state.getCurrentStep());
            return;
        }

        if ("SUCCESS".equals(event.getStatus())) {
            if (event.getData() != null) {
                state.getCompensationData().put(event.getStep(), event.getData());
            }
            state.getCompletedSteps().add(event.getStep());
            sagaStateRepository.save(state);
            proceedToNextStep(state);
        } else {
            log.error("Step {} failed for saga {}. Starting compensation.", event.getStep(), event.getSagaId());
            state.setStatus(SagaState.STATUS_COMPENSATING);
            state.setFailureReason(event.getData() != null ? String.valueOf(event.getData().get("reason")) : "unknown");
            sagaStateRepository.save(state);
            rollback(state);
        }
    }

    public void failAndRollback(SagaState state, String reason) {
        state.setStatus(SagaState.STATUS_COMPENSATING);
        state.setFailureReason(reason);
        sagaStateRepository.save(state);
        rollback(state);
    }

    private void rollback(SagaState state) {
        List<String> completed = state.getCompletedSteps();
        for (int i = completed.size() - 1; i >= 0; i--) {
            String step = completed.get(i);
            StepHandler handler = findHandler(step);
            Map<String, Object> compData = (Map<String, Object>) state.getCompensationData().getOrDefault(step, state.getPayload());
            SagaEvent rollbackEvent = new SagaEvent(state.getSagaId(), step, "ROLLBACK", compData);
            handler.processRollback(rollbackEvent);
        }

        StepHandler userHandler = findHandler("USER");
        SagaEvent userRollback = new SagaEvent(state.getSagaId(), "USER", "ROLLBACK", state.getPayload());
        userHandler.processRollback(userRollback);

        state.setCurrentStep("COMPENSATING");
        sagaStateRepository.save(state);
    }

    private void handleRollbackDone(SagaState state, SagaEvent event) {
        log.info("Compensation step {} confirmed for saga {}", event.getStep(), state.getSagaId());
        if ("USER".equals(event.getStep())) {
            state.setStatus(SagaState.STATUS_COMPENSATED);
            state.setCurrentStep("COMPENSATED");
            sagaStateRepository.save(state);
            log.info("Saga {} fully compensated", state.getSagaId());
        }
    }

    private boolean tryMarkProcessed(String dedupeKey) {
        Boolean isNew = stringRedisTemplate.opsForValue().setIfAbsent("saga:dedupe:" + dedupeKey, "1", DEDUPE_TTL);
        return Boolean.TRUE.equals(isNew);
    }

    private StepHandler findHandler(String step) {
        return stepHandlers.values().stream()
                .filter(h -> h.getStepName().equals(step))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Handler not found for step " + step));
    }
}