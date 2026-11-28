package org.example.controller;

import org.example.model.SagaState;
import org.example.repository.SagaStateRepository;
import org.example.service.SagaOrchestrator;
import org.example.service.SagaOutboxService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/sagas")
public class SagaController {

    @Autowired
    private SagaOrchestrator orchestrator;

    @Autowired
    private SagaStateRepository sagaStateRepository;

    @PostMapping("/create-program")
    public ResponseEntity<SagaState> createProgram(@RequestHeader("X-User-Id") String userId,
                                                   @RequestBody Map<String, Object> payload) {
        SagaState state = orchestrator.startSaga(SagaOrchestrator.SAGA_TYPE, payload, userId);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(state);
    }

    @PostMapping("/outbox/create-program")
    public ResponseEntity<SagaState> createProgramWithOutbox(@RequestHeader("X-User-Id") String userId,
                                                              @RequestBody Map<String, Object> payload) {
        SagaState state = orchestrator.startSagaWithOutbox(
                SagaOrchestrator.SAGA_TYPE, payload, userId);
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
