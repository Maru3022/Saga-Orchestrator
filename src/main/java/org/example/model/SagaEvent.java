package org.example.model;

import java.io.Serializable;
import java.util.Map;

public class SagaEvent implements Serializable {

    private String sagaId;
    private String stepName;
    private String status;
    private Map<String, Object> data;
    private String commandId;
    private String operation;
    private Integer eventVersion;

    public SagaEvent() {}

    public SagaEvent(String sagaId, String stepName, String status, Map<String, Object> data) {
        this.sagaId = sagaId;
        this.stepName = stepName;
        this.status = status;
        this.data = data;
    }

    public String getSagaId() {
        return sagaId;
    }

    public void setSagaId(String sagaId) {
        this.sagaId = sagaId;
    }

    public String getStepName() {
        return stepName;
    }

    public void setStepName(String stepName) {
        this.stepName = stepName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public String getCommandId() { return commandId; }
    public void setCommandId(String commandId) { this.commandId = commandId; }
    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }
    public Integer getEventVersion() { return eventVersion; }
    public void setEventVersion(Integer eventVersion) { this.eventVersion = eventVersion; }
}