package org.example.controller;

import org.example.model.SagaState;
import org.example.service.SagaOrchestrator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/saga")
public class SagaController {

    @Autowired
    private SagaOrchestrator orchestrator;

    @PostMapping("/create-program")
    public ResponseEntity<SagaState> createProgram(@RequestBody Map<String, Object> payload) {
        SagaState state = orchestrator.startSaga("CREATE_PROGRAM", payload);
        return ResponseEntity.accepted().body(state);
    }
}
