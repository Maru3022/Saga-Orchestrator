package org.example.temporal;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface CreateProgramWorkflow {
    @WorkflowMethod
    String createProgram(CreateProgramInput input);

    @QueryMethod
    String getStatus();
}
