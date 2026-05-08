package org.example.model;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

public class SagaState implements Serializable {

    private String sagaId;
    private String SagaType;
    private String currentStep;
    private Map<String,Object> payload;
    private Map<String,Object> compensationData;
    private String failureReason;

    public SagaState(){}

    public SagaState(String sagaType, Map<String,Object> payload){
        this.sagaId = UUID.randomUUID().toString();
        this.SagaType = sagaType;
        this.payload = payload;
        this.currentStep = "STARTED";
    }

    public void setSagaId(String sagaId) {
        this.sagaId = sagaId;
    }

    public void setSagaType(String sagaType) {
        SagaType = sagaType;
    }

    public void setCurrentStep(String currentStep) {
        this.currentStep = currentStep;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    public void setCompensationData(Map<String, Object> compensationData) {
        this.compensationData = compensationData;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public String getSagaId() {
        return sagaId;
    }

    public String getSagaType() {
        return SagaType;
    }

    public String getCurrentStep() {
        return currentStep;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public Map<String, Object> getCompensationData() {
        return compensationData;
    }

    public String getFailureReason() {
        return failureReason;
    }
}
