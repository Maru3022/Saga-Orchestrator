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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
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

        Mockito.when(orchestrator.startSaga(eq(SagaOrchestrator.SAGA_TYPE), any(), eq("user-123"))).thenReturn(mockState);

        mockMvc.perform(post("/api/v1/sagas/create-program")
                        .header("X-User-Id", "user-123")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":\"123\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.sagaId").isNotEmpty())
                .andExpect(jsonPath("$.sagaType").value(SagaOrchestrator.SAGA_TYPE))
                .andExpect(jsonPath("$.currentStep").value("STARTED"));
    }

    @Test
    void shouldForbidReadingAnotherUsersSaga() throws Exception {
        SagaState state = new SagaState(SagaOrchestrator.SAGA_TYPE, Map.of("private", "value"));
        state.setSagaId("saga-1");
        state.setOwnerId("victim");
        Mockito.when(sagaStateRepository.findById("saga-1")).thenReturn(java.util.Optional.of(state));

        mockMvc.perform(get("/api/v1/sagas/saga-1")
                        .header("X-User-Id", "attacker"))
                .andExpect(status().isForbidden());
    }

    @Test
    void shouldReturnNotFoundWhenSagaDoesNotExist() throws Exception {
        Mockito.when(sagaStateRepository.findById("missing")).thenReturn(java.util.Optional.empty());

        mockMvc.perform(get("/api/v1/sagas/missing")
                        .header("X-User-Id", "user-123"))
                .andExpect(status().isNotFound());
    }

    @Test
    void shouldReturnSagaForItsOwner() throws Exception {
        SagaState state = new SagaState(SagaOrchestrator.SAGA_TYPE, Map.of("goal", "fitness"));
        state.setSagaId("saga-1");
        state.setOwnerId("user-123");
        Mockito.when(sagaStateRepository.findById("saga-1")).thenReturn(java.util.Optional.of(state));

        mockMvc.perform(get("/api/v1/sagas/saga-1")
                        .header("X-User-Id", "user-123"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sagaId").value("saga-1"));
    }

    @Test
    void shouldReturnUnavailableWhenTemporalIsDisabled() throws Exception {
        mockMvc.perform(post("/api/v1/sagas/temporal/create-program")
                        .header("X-User-Id", "user-123")
                        .header("Idempotency-Key", "request-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"goal\":\"fitness\"}"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("Temporal is disabled"));
    }
}
