package org.example.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SagaState implements Serializable {

    public static final String STATUS_IN_PROGRESS = "IN_PROGRESS";
    public static final String STATUS_COMPLETED = "COMPLETED";
    public static final String STATUS_FAILED = "FAILED";
    public static final String STATUS_COMPENSATING = "COMPENSATING";
    public static final String STATUS_COMPENSATED = "COMPENSATED";

    private String sagaId;
    private String sagaType;
    private String currentStep;
    private String status;
    private List<String> completedSteps = new ArrayList<>();
    private Map<String, Object> payload;
    private Map<String, Object> compensationData = new HashMap<>();
    private String failureReason;
    private long createdAt;
    private long updatedAt;

    public SagaState() {}

    public SagaState(String sagaType, Map<String, Object> payload) {
        this.sagaId = UUID.randomUUID().toString();
        this.sagaType = sagaType;
        this.payload = payload != null ? payload : new HashMap<>();
        this.currentStep = "STARTED";
        this.status = STATUS_IN_PROGRESS;
        this.createdAt = System.currentTimeMillis();
        this.updatedAt = this.createdAt;
    }

    public void touch() {
        this.updatedAt = System.currentTimeMillis();
    }

    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }

    public String getSagaType() { return sagaType; }
    public void setSagaType(String sagaType) { this.sagaType = sagaType; }

    public String getCurrentStep() { return currentStep; }
    public void setCurrentStep(String currentStep) { this.currentStep = currentStep; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public List<String> getCompletedSteps() { return completedSteps; }
    public void setCompletedSteps(List<String> completedSteps) { this.completedSteps = completedSteps; }

    public Map<String, Object> getPayload() { return payload; }
    public void setPayload(Map<String, Object> payload) { this.payload = payload; }

    public Map<String, Object> getCompensationData() { return compensationData; }
    public void setCompensationData(Map<String, Object> compensationData) { this.compensationData = compensationData; }

    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }

    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }

    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
}