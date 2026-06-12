package org.example.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class SagaState implements Serializable {

    public static final String STATUS_IN_PROGRESS = "IN_PROGRESS";
    public static final String STATUS_COMPLETED = "COMPLETED";
    public static final String STATUS_COMPENSATING = "COMPENSATING";
    public static final String STATUS_FAILED = "FAILED";

    private String sagaId;
    private String sagaType;
    private String currentStep;
    private String status;
    private String failureReason;
    private Map<String, Object> payload;
    private long createdAt;
    private long updatedAt;

    public SagaState() {}

    public SagaState(String sagaType, Map<String, Object> payload) {
        this.sagaId = UUID.randomUUID().toString();
        this.sagaType = sagaType;
        this.payload = payload;
        this.status = STATUS_IN_PROGRESS;
        this.createdAt = Instant.now().toEpochMilli();
        this.updatedAt = this.createdAt;
    }

    public void touch() {
        this.updatedAt = Instant.now().toEpochMilli();
    }

    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }

    public String getSagaType() { return sagaType; }
    public void setSagaType(String sagaType) { this.sagaType = sagaType; }

    public String getCurrentStep() { return currentStep; }
    public void setCurrentStep(String currentStep) { this.currentStep = currentStep; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }

    public Map<String, Object> getPayload() { return payload; }
    public void setPayload(Map<String, Object> payload) { this.payload = payload; }

    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }

    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
}
