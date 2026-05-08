package org.example.model;

import java.util.List;

public class SagaDefinition {

    private String sagaType;
    private List<String> forwardSteps;
    private List<String> rollbackSteps;

    public SagaDefinition(String sagaType, List<String> forwardSteps, List<String> rollbackSteps) {
        this.sagaType = sagaType;
        this.forwardSteps = forwardSteps;
        this.rollbackSteps = rollbackSteps;
    }

    public String getSagaType() {
        return sagaType;
    }

    public List<String> getForwardSteps() {
        return forwardSteps;
    }

    public List<String> getRollbackSteps() {
        return rollbackSteps;
    }
}
