package org.example.temporal;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.CanceledFailure;
import io.temporal.failure.ApplicationFailure;
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
                StepOutcome outcome = activities.executeStep(input, step);
                if (!outcome.isSuccess()) {
                    status = "COMPENSATING";
                    String compensationFailure = compensate(input, completed);
                    status = compensationFailure == null ? "FAILED" : "COMPENSATION_FAILED";
                    throw ApplicationFailure.newFailure(
                            "Downstream step failed: " + step + ": " + outcome.getMessage(),
                            "FitFlowStepFailure",
                            step,
                            outcome.getMessage(),
                            compensationFailure);
                }
                completed.add(step);
            }
            status = "COMPLETED";
            return "COMPLETED";
        } catch (ActivityFailure failure) {
            String compensationFailure = compensate(input, completed);
            status = compensationFailure == null ? "FAILED" : "COMPENSATION_FAILED";
            throw Workflow.wrap(failure);
        } catch (CanceledFailure cancellation) {
            String compensationFailure = compensate(input, completed);
            status = compensationFailure == null ? "CANCELLED" : "COMPENSATION_FAILED";
            throw Workflow.wrap(cancellation);
        }
    }

    private String compensate(CreateProgramInput input, List<String> completed) {
            status = "COMPENSATING";
            String compensationFailure = null;
            for (int i = completed.size() - 1; i >= 0; i--) {
                try {
                    activities.compensateStep(input, completed.get(i));
                } catch (RuntimeException currentFailure) {
                    status = "COMPENSATION_FAILED";
                    if (compensationFailure == null) {
                        compensationFailure = currentFailure.getMessage();
                    }
                }
            }
            return compensationFailure;
    }

    @Override
    public String getStatus() {
        return status;
    }
}
