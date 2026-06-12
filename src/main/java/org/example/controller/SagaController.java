package org.example.controller;

import org.example.model.SagaState;
import org.example.repository.SagaStateRepository;
import org.example.service.SagaOrchestrator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/saga")
public class SagaController {

    @Autowired
    private SagaOrchestrator orchestrator;

    @Autowired
    private SagaStateRepository sagaStateRepository;

    @PostMapping("/create-program")
    public ResponseEntity<SagaState> createProgram(@RequestBody Map<String, Object> payload) {
        SagaState state = orchestrator.startSaga(SagaOrchestrator.SAGA_TYPE, payload);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(state);
    }

    @GetMapping("/{sagaId}")
    public ResponseEntity<SagaState> getSaga(@PathVariable String sagaId) {
        return sagaStateRepository.findById(sagaId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}