package controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.SagaOrchestratorApplication;
import org.example.controller.SagaController;
import org.example.model.SagaState;
import org.example.repository.SagaStateRepository;
import org.example.service.SagaOrchestrator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(controllers = SagaController.class)
@ContextConfiguration(classes = SagaOrchestratorApplication.class)
@ActiveProfiles("test")
class SagaControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private SagaOrchestrator orchestrator;

    @MockBean
    private SagaStateRepository sagaStateRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void shouldStartSagaAndReturnAccepted() throws Exception {
        SagaState mockState = new SagaState(SagaOrchestrator.SAGA_TYPE, Map.of("userId", "123"));
        mockState.setSagaId(UUID.randomUUID().toString());
        mockState.setCurrentStep("STARTED");

        Mockito.when(orchestrator.startSaga(eq(SagaOrchestrator.SAGA_TYPE), any())).thenReturn(mockState);

        mockMvc.perform(post("/saga/create-program")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":\"123\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.sagaId").isNotEmpty())
                .andExpect(jsonPath("$.sagaType").value(SagaOrchestrator.SAGA_TYPE))
                .andExpect(jsonPath("$.currentStep").value("STARTED"));
    }
}