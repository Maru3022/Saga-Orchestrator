package org.example.temporal;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class CreateProgramWorkflowImpl implements CreateProgramWorkflow {

    public static final String TASK_QUEUE = "FITFLOW-CREATE-PROGRAM";

    private String status = "STARTED";

    private final CreateProgramActivities activities =
            Workflow.newActivityStub(
                    CreateProgramActivities.class,
                    ActivityOptions.newBuilder()
                            .setStartToCloseTimeout(Duration.ofSeconds(30))
                            .setRetryOptions(RetryOptions.newBuilder()
                                    .setInitialInterval(Duration.ofSeconds(1))
                                    .setMaximumInterval(Duration.ofSeconds(30))
                                    .setBackoffCoefficient(2.0)
                                    .setMaximumAttempts(5)
                                    .build())
                            .build());

    @Override
    public String createProgram(CreateProgramInput input) {
        List<String> completed = new ArrayList<>();
        status = "IN_PROGRESS";
        try {
            for (String step : List.of("TRAINING", "NUTRITION", "NOTIFICATION")) {
                activities.executeStep(input, step);
                completed.add(step);
            }
            status = "COMPLETED";
            return "COMPLETED";
        } catch (ActivityFailure failure) {
            status = "COMPENSATING";
            for (int i = completed.size() - 1; i >= 0; i--) {
                try {
                    activities.compensateStep(input, completed.get(i));
                } catch (RuntimeException compensationFailure) {
                    status = "COMPENSATION_FAILED";
                    throw Workflow.wrap(compensationFailure);
                }
            }
            status = "FAILED";
            throw Workflow.wrap(failure);
        }
    }

    @Override
    public String getStatus() {
        return status;
    }
}
