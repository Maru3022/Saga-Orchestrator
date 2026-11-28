package org.example.model;

import java.io.Serializable;
import java.util.Map;

public class SagaResponseEvent implements Serializable {

    private String eventId;
    private String sagaId;
    private String step;
    private String status;
    private Map<String, Object> data;
    private String commandId;
    private String operation;
    private Integer eventVersion;
    private String errorCode;

    public SagaResponseEvent() {}

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }
    public String getStep() { return step; }
    public void setStep(String step) { this.step = step; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public Map<String, Object> getData() { return data; }
    public void setData(Map<String, Object> data) { this.data = data; }
    public String getCommandId() { return commandId; }
    public void setCommandId(String commandId) { this.commandId = commandId; }
    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }
    public Integer getEventVersion() { return eventVersion; }
    public void setEventVersion(Integer eventVersion) { this.eventVersion = eventVersion; }
    public String getErrorCode() { return errorCode; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }
}