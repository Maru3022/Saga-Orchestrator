package org.example.temporal;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.CanceledFailure;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.Workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CreateProgramWorkflowImpl implements CreateProgramWorkflow {

    public static final String TASK_QUEUE = "FITFLOW-CREATE-PROGRAM";
    private static final List<String> STEPS =
            List.of("TRAINING", "NUTRITION", "NOTIFICATION");

    private String status = "STARTED";
    private String sagaId;
    private String activeOperationKey;
    private String activeCommandId;
    private final Map<String, StepState> stepStates = new LinkedHashMap<>();
    private final Map<String, StepOutcome> outcomes = new HashMap<>();
    private final Set<String> eventIds = new HashSet<>();

    private final CreateProgramActivities activities =
            Workflow.newActivityStub(
                    CreateProgramActivities.class,
                    ActivityOptions.newBuilder()
                            .setStartToCloseTimeout(TemporalTimeouts.ACTIVITY_START_TO_CLOSE)
                            .setScheduleToCloseTimeout(TemporalTimeouts.ACTIVITY_SCHEDULE_TO_CLOSE)
                            .setRetryOptions(RetryOptions.newBuilder()
                                    .setInitialInterval(java.time.Duration.ofSeconds(1))
                                    .setMaximumInterval(java.time.Duration.ofSeconds(30))
                                    .setBackoffCoefficient(2.0)
                                    .setMaximumAttempts(5)
                                    .build())
                            .build());

    @Override
    public String createProgram(CreateProgramInput input) {
        sagaId = input.getSagaId();
        initializeStepStates();
        outcomes.keySet().removeIf(commandId -> !commandId.startsWith(sagaId + ":"));
        List<String> completed = new ArrayList<>();
        status = "IN_PROGRESS";
        try {
            for (String step : STEPS) {
                StepOutcome outcome = dispatchAndAwait(input, step, "EXECUTE");
                if (outcome == null) {
                    status = "UNKNOWN";
                    return status;
                }
                if (!outcome.isSuccess()) {
                    status = "COMPENSATING";
                    CompensationResult compensation = compensate(input, completed);
                    status = switch (compensation) {
                        case COMPENSATED -> "FAILED";
                        case FAILED -> "COMPENSATION_FAILED";
                        case UNKNOWN -> "UNKNOWN";
                    };
                    return status;
                }
                completed.add(step);
            }
            status = "COMPLETED";
            return status;
        } catch (CanceledFailure cancellation) {
            if (activeCommandId != null && !outcomes.containsKey(activeCommandId)) {
                stepStates.put(activeOperationKey, StepState.UNKNOWN);
                status = "UNKNOWN";
            } else {
                List<String> confirmedSteps = successfulExecutionSteps();
                String[] compensationResult = new String[1];
                CancellationScope detachedScope = Workflow.newDetachedCancellationScope(
                        () -> compensationResult[0] = compensate(input, confirmedSteps).name());
                detachedScope.run();
                status = compensationResult[0] == null
                        ? "CANCELLED"
                        : compensationResult[0];
            }
            throw Workflow.wrap(cancellation);
        }
    }

    private StepOutcome dispatchAndAwait(
            CreateProgramInput input, String step, String operation) {
        String stateKey = stateKey(step, operation);
        String commandId = TemporalCommandIds.forStep(input.getSagaId(), step, operation);
        activeOperationKey = stateKey;
        activeCommandId = commandId;
        status = "DISPATCHING_" + step;
        try {
            if ("COMPENSATE".equals(operation)) {
                activities.compensateStep(input, step);
            } else {
                activities.executeStep(input, step);
            }
            if (stepStates.get(stateKey) == StepState.NOT_STARTED) {
                stepStates.put(stateKey, StepState.DISPATCHED);
            }
        } catch (ActivityFailure failure) {
            if (!outcomes.containsKey(commandId)) {
                stepStates.put(stateKey, StepState.UNKNOWN);
            }
        }

        StepOutcome outcome = awaitOutcome(input, step, operation);
        activeCommandId = null;
        activeOperationKey = null;
        return outcome;
    }

    private CompensationResult compensate(CreateProgramInput input, List<String> completed) {
        status = "COMPENSATING";
        boolean failed = false;
        for (int i = completed.size() - 1; i >= 0; i--) {
            String step = completed.get(i);
            if (stepStates.get(stateKey(step, "EXECUTE")) != StepState.SUCCEEDED) {
                continue;
            }
            StepOutcome outcome = dispatchAndAwait(input, step, "COMPENSATE");
            if (outcome == null) {
                return CompensationResult.UNKNOWN;
            }
            if (!outcome.isSuccess()) {
                failed = true;
            }
        }
        return failed ? CompensationResult.FAILED : CompensationResult.COMPENSATED;
    }

    private StepOutcome awaitOutcome(CreateProgramInput input, String step, String operation) {
        String commandId = TemporalCommandIds.forStep(input.getSagaId(), step, operation);
        String operationKey = stateKey(step, operation);
        status = "WAITING_FOR_" + step + "_" + operation;
        boolean received = Workflow.await(
                TemporalTimeouts.OUTCOME_TIMEOUT,
                () -> outcomes.containsKey(commandId));
        if (!received) {
            stepStates.put(operationKey, StepState.UNKNOWN);
            status = "UNKNOWN";
            received = Workflow.await(
                    TemporalTimeouts.OUTCOME_RECOVERY_TIMEOUT,
                    () -> outcomes.containsKey(commandId));
        }
        return received ? outcomes.get(commandId) : null;
    }

    @Override
    public void receiveOutcome(StepOutcomeSignal outcome) {
        if (outcome == null || outcome.getSagaId() == null
                || !outcome.getSagaId().equals(sagaId)
                || outcome.getEventId() == null || outcome.getEventId().isBlank()
                || outcome.getStep() == null || outcome.getOperation() == null
                || outcome.getCommandId() == null || outcome.getStatus() == null
                || eventIds.contains(outcome.getEventId())) {
            return;
        }
        String operation = outcome.getOperation().toUpperCase(java.util.Locale.ROOT);
        if (!"EXECUTE".equals(operation) && !"COMPENSATE".equals(operation)) {
            return;
        }
        String expectedCommandId = TemporalCommandIds.forStep(
                outcome.getSagaId(), outcome.getStep(), operation);
        if (!expectedCommandId.equals(outcome.getCommandId())
                || outcomes.containsKey(expectedCommandId)) {
            return;
        }
        String normalizedStep = normalizeStep(outcome.getStep());
        if (normalizedStep == null) {
            return;
        }
        String operationKey = stateKey(normalizedStep, operation);
        StepState currentState = stepStates.get(operationKey);
        if (currentState == null
                || (currentState == StepState.NOT_STARTED
                && !operationKey.equals(activeOperationKey))
                || (currentState != StepState.NOT_STARTED
                && currentState != StepState.DISPATCHED
                && currentState != StepState.UNKNOWN)) {
            return;
        }
        StepState terminalState = terminalState(operation, outcome.getStatus());
        if (terminalState == null) {
            return;
        }
        eventIds.add(outcome.getEventId());
        outcomes.put(expectedCommandId,
                new StepOutcome(terminalState == StepState.SUCCEEDED
                        || terminalState == StepState.COMPENSATED, outcome.getStatus()));
        stepStates.put(operationKey, terminalState);
    }

    private void initializeStepStates() {
        stepStates.clear();
        for (String step : STEPS) {
            stepStates.put(stateKey(step, "EXECUTE"), StepState.NOT_STARTED);
            stepStates.put(stateKey(step, "COMPENSATE"), StepState.NOT_STARTED);
        }
    }

    private List<String> successfulExecutionSteps() {
        return STEPS.stream()
                .filter(step -> stepStates.get(stateKey(step, "EXECUTE")) == StepState.SUCCEEDED)
                .toList();
    }

    private String stateKey(String step, String operation) {
        return normalizeStep(step) + ":" + operation;
    }

    private String normalizeStep(String step) {
        if (step == null) {
            return null;
        }
        return switch (step.toUpperCase(java.util.Locale.ROOT)) {
            case "TRAIN", "TRAINS", "TRAINING" -> "TRAINING";
            case "NUTRITION" -> "NUTRITION";
            case "NOTIFICATION" -> "NOTIFICATION";
            default -> null;
        };
    }

    private StepState terminalState(String operation, String status) {
        if ("EXECUTE".equals(operation)) {
            if ("SUCCESS".equalsIgnoreCase(status)) {
                return StepState.SUCCEEDED;
            }
            if ("FAILED".equalsIgnoreCase(status)) {
                return StepState.FAILED;
            }
        } else if ("COMPENSATE".equals(operation)) {
            if ("ROLLBACK_DONE".equalsIgnoreCase(status)) {
                return StepState.COMPENSATED;
            }
            if ("FAILED".equalsIgnoreCase(status)) {
                return StepState.FAILED;
            }
        }
        return null;
    }

    @Override
    public String getStatus() {
        return status;
    }

    @Override
    public Map<String, StepState> getStepStates() {
        return new LinkedHashMap<>(stepStates);
    }

    private enum CompensationResult {
        COMPENSATED,
        FAILED,
        UNKNOWN
    }
}
