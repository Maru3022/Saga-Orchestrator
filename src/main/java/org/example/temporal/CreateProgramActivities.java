package org.example.temporal;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface CreateProgramActivities {
    @ActivityMethod
    void executeStep(CreateProgramInput input, String stepName);

    @ActivityMethod
    void compensateStep(CreateProgramInput input, String stepName);
}
