package org.example.temporal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.client.WorkflowClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.example.model.SagaResponseEvent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class TemporalSagaResponseListenerTest {

    private WorkflowClient workflowClient;
    private CreateProgramWorkflow workflow;
    private TemporalSagaResponseListener listener;

    @BeforeEach
    void setUp() {
        workflowClient = mock(WorkflowClient.class);
        workflow = mock(CreateProgramWorkflow.class);
        listener = new TemporalSagaResponseListener(new ObjectMapper(), workflowClient);
        when(workflowClient.newWorkflowStub(
                eq(CreateProgramWorkflow.class), any(String.class))).thenReturn(workflow);
    }

    @Test
    void signalsWorkflowWithNormalizedStepAndStableCommandId() {
        listener.onResponse(response("temporal-operation-1", "TRAINING", "SUCCESS"));

        var outcome = captureOutcome();
        assertEquals("response-1", outcome.getEventId());
        assertEquals("temporal-operation-1", outcome.getSagaId());
        assertEquals("TRAINING", outcome.getStep());
        assertEquals("EXECUTE", outcome.getOperation());
        assertEquals("temporal-operation-1:TRAINS:EXECUTE", outcome.getCommandId());
        assertEquals("SUCCESS", outcome.getStatus());
    }

    @Test
    void mapsCompensationConfirmationToCompensateOperation() {
        listener.onResponse(response("temporal-operation-1", "NUTRITION", "ROLLBACK_DONE"));

        var outcome = captureOutcome();
        assertEquals("COMPENSATE", outcome.getOperation());
        assertEquals("ROLLBACK_DONE", outcome.getStatus());
        assertEquals("temporal-operation-1:NUTRITION:ROLLBACK", outcome.getCommandId());
    }

    @Test
    void ignoresLegacyAndUnrecognizedResponses() {
        listener.onResponse(response("legacy-saga", "TRAINING", "SUCCESS"));
        listener.onResponse(response("temporal-operation-1", "UNKNOWN", "SUCCESS"));
        listener.onResponse("{\"sagaId\":\"temporal-operation-1\",\"step\":\"TRAINING\","
                + "\"status\":\"SUCCESS\"}");

        verifyNoInteractions(workflowClient);
    }

    @Test
    void propagatesSignalDeliveryFailureSoResponseCanBeRetried() {
        doThrow(new IllegalStateException("Temporal unavailable"))
                .when(workflow).receiveOutcome(any(StepOutcomeSignal.class));

        assertThrows(IllegalStateException.class,
                () -> listener.onResponse(response("temporal-operation-1", "TRAINING", "SUCCESS")));
    }

    @Test
    void rejectsMalformedResponseJson() {
        assertThrows(IllegalArgumentException.class, () -> listener.onResponse("{"));
        verifyNoInteractions(workflowClient);
    }

    private StepOutcomeSignal captureOutcome() {
        var captor = org.mockito.ArgumentCaptor.forClass(StepOutcomeSignal.class);
        verify(workflow).receiveOutcome(captor.capture());
        return captor.getValue();
    }

    private String response(String sagaId, String step, String status) {
        return "{\"eventId\":\"response-1\",\"sagaId\":\"" + sagaId
                + "\",\"step\":\"" + step + "\",\"status\":\"" + status + "\"}";
    }
}
