package org.example.temporal;

public class StepOutcomeSignal {
    private String eventId;
    private String sagaId;
    private String step;
    private String operation;
    private String commandId;
    private String status;

    public StepOutcomeSignal() {}

    public StepOutcomeSignal(String eventId, String sagaId, String step,
                             String operation, String commandId, String status) {
        this.eventId = eventId;
        this.sagaId = sagaId;
        this.step = step;
        this.operation = operation;
        this.commandId = commandId;
        this.status = status;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }
    public String getStep() { return step; }
    public void setStep(String step) { this.step = step; }
    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }
    public String getCommandId() { return commandId; }
    public void setCommandId(String commandId) { this.commandId = commandId; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
}
