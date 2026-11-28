package org.example.temporal;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.CanceledFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CreateProgramWorkflowImpl implements CreateProgramWorkflow {

    public static final String TASK_QUEUE = "FITFLOW-CREATE-PROGRAM";

    private String status = "STARTED";
    private String sagaId;
    private final Map<String, StepOutcome> outcomes = new HashMap<>();
    private final Set<String> eventIds = new HashSet<>();

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
        sagaId = input.getSagaId();
        outcomes.keySet().removeIf(commandId -> !commandId.startsWith(sagaId + ":"));
        List<String> completed = new ArrayList<>();
        status = "IN_PROGRESS";
        try {
            for (String step : List.of("TRAINING", "NUTRITION", "NOTIFICATION")) {
                completed.add(step);
                activities.executeStep(input, step);
                StepOutcome outcome = awaitOutcome(input, step, "EXECUTE");
                if (outcome == null) {
                    status = "COMPENSATING";
                    String compensationFailure = compensate(input, completed);
                    status = compensationFailure == null ? "FAILED" : "COMPENSATION_FAILED";
                    throw ApplicationFailure.newFailure(
                            "Timed out waiting for downstream step: " + step,
                            "FitFlowStepTimeout",
                            step,
                            compensationFailure);
                }
                if (!outcome.isSuccess()) {
                    completed.remove(completed.size() - 1);
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
            }
            status = "COMPLETED";
            return "COMPLETED";
        } catch (ActivityFailure failure) {
            status = "COMPENSATING";
            String compensationFailure = compensate(input, completed);
            status = compensationFailure == null ? "FAILED" : "COMPENSATION_FAILED";
            throw Workflow.wrap(failure);
        } catch (CanceledFailure cancellation) {
            String[] compensationFailure = new String[1];
            CancellationScope detachedScope = Workflow.newDetachedCancellationScope(
                    () -> compensationFailure[0] = compensate(input, completed));
            detachedScope.run();
            status = compensationFailure[0] == null ? "CANCELLED" : "COMPENSATION_FAILED";
            throw Workflow.wrap(cancellation);
        }
    }

    private String compensate(CreateProgramInput input, List<String> completed) {
        status = "COMPENSATING";
        String compensationFailure = null;
        for (int i = completed.size() - 1; i >= 0; i--) {
            String step = completed.get(i);
            try {
                activities.compensateStep(input, step);
                StepOutcome outcome = awaitOutcome(input, step, "COMPENSATE");
                if (outcome == null || !outcome.isSuccess()) {
                    throw new IllegalStateException(outcome == null
                            ? "Timed out waiting for compensation outcome for " + step
                            : outcome.getMessage());
                }
            } catch (RuntimeException currentFailure) {
                status = "COMPENSATION_FAILED";
                if (compensationFailure == null) {
                    compensationFailure = currentFailure.getMessage();
                }
            }
        }
        return compensationFailure;
    }

    private StepOutcome awaitOutcome(CreateProgramInput input, String step, String operation) {
        String commandId = TemporalCommandIds.forStep(input.getSagaId(), step, operation);
        status = "WAITING_FOR_" + step;
        boolean received = Workflow.await(
                Duration.ofMinutes(2),
                () -> outcomes.containsKey(commandId));
        if (!received) {
            return null;
        }
        return outcomes.get(commandId);
    }

    @Override
    public void receiveOutcome(StepOutcomeSignal outcome) {
        if (outcome == null || outcome.getSagaId() == null
                || (sagaId != null && !sagaId.equals(outcome.getSagaId()))
                || outcome.getEventId() == null || outcome.getEventId().isBlank()
                || outcome.getStep() == null || outcome.getOperation() == null
                || (!"EXECUTE".equalsIgnoreCase(outcome.getOperation())
                && !"COMPENSATE".equalsIgnoreCase(outcome.getOperation()))
                || outcome.getCommandId() == null || outcome.getStatus() == null
                || eventIds.contains(outcome.getEventId())) {
            return;
        }
        String expectedCommandId = TemporalCommandIds.forStep(
                outcome.getSagaId(), outcome.getStep(), outcome.getOperation());
        if (!expectedCommandId.equals(outcome.getCommandId())) {
            return;
        }
        eventIds.add(outcome.getEventId());
        if (outcomes.containsKey(expectedCommandId)) {
            return;
        }
        boolean success = "SUCCESS".equalsIgnoreCase(outcome.getStatus())
                && "EXECUTE".equalsIgnoreCase(outcome.getOperation())
                || "ROLLBACK_DONE".equalsIgnoreCase(outcome.getStatus())
                && "COMPENSATE".equalsIgnoreCase(outcome.getOperation());
        outcomes.put(expectedCommandId, new StepOutcome(success, outcome.getStatus()));
    }

    @Override
    public String getStatus() {
        return status;
    }
}
