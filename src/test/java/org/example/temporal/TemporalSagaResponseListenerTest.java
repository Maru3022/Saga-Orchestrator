package org.example.temporal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowNotFoundException;
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
    void acknowledgesLateOutcomeForWorkflowNoLongerRetained() {
        doThrow(new WorkflowNotFoundException(
                WorkflowExecution.newBuilder()
                        .setWorkflowId("fitflow-create-program-operation-1")
                        .build(),
                "default",
                null))
                .when(workflow).receiveOutcome(any(StepOutcomeSignal.class));

        org.junit.jupiter.api.Assertions.assertDoesNotThrow(
                () -> listener.onResponse(response("temporal-operation-1", "TRAINING", "SUCCESS")));
    }

    @Test
    void rejectsMalformedResponseJson() {
        assertThrows(IllegalArgumentException.class, () -> listener.onResponse("{"));
        verifyNoInteractions(workflowClient);
    }

    @Test
    void ignoresResponseWithMismatchedCommandOrOperation() {
        listener.onResponse("{\"eventId\":\"response-1\",\"sagaId\":\"temporal-operation-1\","
                + "\"step\":\"TRAINING\",\"status\":\"SUCCESS\",\"commandId\":\"wrong\","
                + "\"operation\":\"EXECUTE\",\"eventVersion\":1}");
        listener.onResponse("{\"eventId\":\"response-1\",\"sagaId\":\"temporal-operation-1\","
                + "\"step\":\"TRAINING\",\"status\":\"SUCCESS\","
                + "\"commandId\":\"temporal-operation-1:TRAINS:EXECUTE\","
                + "\"operation\":\"COMPENSATE\",\"eventVersion\":1}");
        listener.onResponse("{\"eventId\":\"response-1\",\"sagaId\":\"temporal-operation-1\","
                + "\"step\":\"TRAINING\",\"status\":\"SUCCESS\","
                + "\"commandId\":\"temporal-operation-1:TRAINS:EXECUTE\","
                + "\"operation\":\"EXECUTE\",\"eventVersion\":2}");

        verifyNoInteractions(workflowClient);
    }

    private StepOutcomeSignal captureOutcome() {
        var captor = org.mockito.ArgumentCaptor.forClass(StepOutcomeSignal.class);
        verify(workflow).receiveOutcome(captor.capture());
        return captor.getValue();
    }

    private String response(String sagaId, String step, String status) {
        String normalizedStep = "TRAINING".equals(step) ? "TRAINS" : step;
        String operation = "ROLLBACK_DONE".equals(status) ? "COMPENSATE" : "EXECUTE";
        String commandOperation = "COMPENSATE".equals(operation) ? "ROLLBACK" : "EXECUTE";
        return "{\"eventId\":\"response-1\",\"sagaId\":\"" + sagaId
                + "\",\"step\":\"" + step + "\",\"status\":\"" + status
                + "\",\"commandId\":\"" + sagaId + ":" + normalizedStep + ":"
                + commandOperation + "\",\"operation\":\"" + operation
                + "\",\"eventVersion\":1}";
    }
}
