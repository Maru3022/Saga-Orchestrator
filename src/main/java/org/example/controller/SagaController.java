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

@RestController
@RequestMapping("/api/v1/sagas")
public class SagaController {

    @Autowired
    private SagaOrchestrator orchestrator;

    @Autowired
    private SagaStateRepository sagaStateRepository;

    @Autowired
    private Optional<WorkflowClient> workflowClient;

    @PostMapping("/temporal/create-program")
    public ResponseEntity<Map<String, String>> startTemporalCreateProgram(
            @RequestHeader("X-User-Id") String userId,
            @RequestBody Map<String, Object> payload) {
        if (workflowClient.isEmpty()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "Temporal is disabled"));
        }
        String sagaId = UUID.randomUUID().toString();
        CreateProgramWorkflow workflow = workflowClient.get().newWorkflowStub(
                CreateProgramWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(CreateProgramWorkflowImpl.TASK_QUEUE)
                        .setWorkflowId("fitflow-create-program-" + sagaId)
                        .setWorkflowExecutionTimeout(java.time.Duration.ofMinutes(5))
                        .build());
        WorkflowClient.start(workflow::createProgram, new CreateProgramInput(sagaId, userId, payload));
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
