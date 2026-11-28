package org.example.controller;

import org.example.model.SagaState;
import org.example.repository.SagaStateRepository;
import org.example.service.SagaOrchestrator;
import org.example.temporal.CreateProgramInput;
import org.example.temporal.CreateProgramWorkflow;
import org.example.temporal.CreateProgramWorkflowImpl;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.temporal.TemporalIdempotencyStore;
import io.temporal.api.enums.v1.WorkflowIdReusePolicy;

@RestController
@RequestMapping("/api/v1/sagas")
public class SagaController {

    @Autowired
    private SagaOrchestrator orchestrator;

    @Autowired
    private SagaStateRepository sagaStateRepository;

    @Autowired
    private Optional<WorkflowClient> workflowClient;

    @Autowired
    private Optional<TemporalIdempotencyStore> temporalIdempotencyStore;

    @Autowired
    private ObjectMapper objectMapper;

    @PostMapping("/temporal/create-program")
    public ResponseEntity<Map<String, String>> startTemporalCreateProgram(
            @RequestHeader("X-User-Id") String userId,
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody Map<String, Object> payload) {
        if (workflowClient.isEmpty()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "Temporal is disabled"));
        }
        String canonicalPayload;
        try {
            canonicalPayload = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Payload cannot be serialized", e);
        }
        TemporalIdempotencyStore.Reservation reservation =
                temporalIdempotencyStore.orElseThrow(
                        () -> new IllegalStateException("Temporal is enabled but idempotency store is unavailable"))
                        .reserve(userId, idempotencyKey, canonicalPayload);
        if (reservation.kind() == TemporalIdempotencyStore.Reservation.Kind.CONFLICT) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "Idempotency-Key is already used with a different payload"));
        }
        String sagaId = userId + ":" + idempotencyKey;
        CreateProgramWorkflow workflow = workflowClient.get().newWorkflowStub(
                CreateProgramWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(CreateProgramWorkflowImpl.TASK_QUEUE)
                        .setWorkflowId("fitflow-create-program-" + sagaId)
                        .setWorkflowExecutionTimeout(java.time.Duration.ofMinutes(5))
                        .setWorkflowIdReusePolicy(
                                WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
                        .build());
        try {
            WorkflowClient.start(workflow::createProgram,
                    new CreateProgramInput(sagaId, userId, payload));
        } catch (io.temporal.client.WorkflowExecutionAlreadyStarted e) {
            if (reservation.kind() == TemporalIdempotencyStore.Reservation.Kind.REUSED) {
                return ResponseEntity.status(HttpStatus.ACCEPTED)
                        .body(Map.of("sagaId", sagaId,
                                "workflowId", "fitflow-create-program-" + sagaId,
                                "status", "ALREADY_STARTED"));
            }
            throw e;
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("sagaId", sagaId,
                        "workflowId", "fitflow-create-program-" + sagaId));
    }

    @PostMapping("/create-program")
    public ResponseEntity<SagaState> createProgram(@RequestHeader("X-User-Id") String userId,
                                                   @RequestBody Map<String, Object> payload) {
        SagaState state = orchestrator.startSaga(SagaOrchestrator.SAGA_TYPE, payload, userId);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(state);
    }

    @GetMapping("/{sagaId}")
    public ResponseEntity<SagaState> getSaga(@RequestHeader("X-User-Id") String userId,
                                             @PathVariable String sagaId) {
        return sagaStateRepository.findById(sagaId)
                .map(state -> userId.equals(state.getOwnerId())
                        ? ResponseEntity.ok(state)
                        : ResponseEntity.status(HttpStatus.FORBIDDEN).<SagaState>build())
                .orElse(ResponseEntity.notFound().build());
    }
}
