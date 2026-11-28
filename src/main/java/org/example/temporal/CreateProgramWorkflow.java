package org.example.temporal;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.util.Map;

@WorkflowInterface
public interface CreateProgramWorkflow {
    @WorkflowMethod
    String createProgram(CreateProgramInput input);

    @QueryMethod
    String getStatus();

    @QueryMethod
    Map<String, StepState> getStepStates();

    @SignalMethod
    void receiveOutcome(StepOutcomeSignal outcome);
}
