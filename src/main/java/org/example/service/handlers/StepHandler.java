package org.example.service.handlers;

import org.example.model.SagaEvent;

public interface StepHandler {

    void processForward(SagaEvent event);
    void processRollback(SagaEvent event);
    String getStepName();
}
