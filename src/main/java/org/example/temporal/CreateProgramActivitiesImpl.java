package org.example.temporal;

import io.temporal.activity.Activity;
import org.example.model.SagaEvent;
import org.example.service.handlers.StepHandler;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.time.Duration;

@Component
public class CreateProgramActivitiesImpl implements CreateProgramActivities {

    private final List<StepHandler> stepHandlers;
    private final TemporalStepOutcomeRegistry outcomeRegistry;

    public CreateProgramActivitiesImpl(List<StepHandler> stepHandlers,
                                       TemporalStepOutcomeRegistry outcomeRegistry) {
        this.stepHandlers = stepHandlers;
        this.outcomeRegistry = outcomeRegistry;
    }

    @Override
    public StepOutcome executeStep(CreateProgramInput input, String stepName) {
        String attemptKey = input.getSagaId() + ":" + stepName + ":EXECUTE";
        Activity.getExecutionContext().heartbeat(attemptKey);
        StepHandler handler = findHandler(stepName);
        handler.processForward(new SagaEvent(
                input.getSagaId(),
                stepName,
                "EXECUTE",
                Map.copyOf(input.getPayload())));
        return outcomeRegistry.await(input.getSagaId(), stepName, Duration.ofMinutes(2).toMillis());
    }

    @Override
    public void compensateStep(CreateProgramInput input, String stepName) {
        String attemptKey = input.getSagaId() + ":" + stepName + ":ROLLBACK";
        Activity.getExecutionContext().heartbeat(attemptKey);
        findHandler(stepName).processRollback(new SagaEvent(
                input.getSagaId(),
                stepName,
                "FAILED",
                Map.copyOf(input.getPayload())));
    }

    private StepHandler findHandler(String stepName) {
        return stepHandlers.stream()
                .filter(handler -> stepName.equals(handler.getStepName()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown step: " + stepName));
    }
}
