package org.example.temporal;

import io.temporal.activity.Activity;
import org.example.model.SagaEvent;
import org.example.service.handlers.StepHandler;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class CreateProgramActivitiesImpl implements CreateProgramActivities {

    private final List<StepHandler> stepHandlers;

    public CreateProgramActivitiesImpl(List<StepHandler> stepHandlers) {
        this.stepHandlers = stepHandlers;
    }

    @Override
    public void executeStep(CreateProgramInput input, String stepName) {
        String commandId = TemporalCommandIds.forStep(input.getSagaId(), stepName, "EXECUTE");
        String attemptKey = commandId;
        Activity.getExecutionContext().heartbeat(attemptKey);
        StepHandler handler = findHandler(stepName);
        SagaEvent event = new SagaEvent(
                input.getSagaId(),
                stepName,
                "EXECUTE",
                Map.copyOf(input.getPayload()));
        event.setCommandId(commandId);
        event.setOperation("EXECUTE");
        event.setEventVersion(1);
        handler.processForward(event);
    }

    @Override
    public void compensateStep(CreateProgramInput input, String stepName) {
        String commandId = TemporalCommandIds.forStep(input.getSagaId(), stepName, "COMPENSATE");
        String attemptKey = commandId;
        Activity.getExecutionContext().heartbeat(attemptKey);
        SagaEvent event = new SagaEvent(
                input.getSagaId(),
                stepName,
                "FAILED",
                Map.copyOf(input.getPayload()));
        event.setCommandId(commandId);
        event.setOperation("COMPENSATE");
        event.setEventVersion(1);
        findHandler(stepName).processRollback(event);
    }

    private StepHandler findHandler(String stepName) {
        String handlerStep = "TRAINING".equals(stepName) ? "TRAINS" : stepName;
        return stepHandlers.stream()
                .filter(handler -> handlerStep.equals(handler.getStepName()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown step: " + stepName));
    }
}
