package org.example.model;

public class SagaEvent {
    private String sagaId;
    private String step;
    private String status;
    private Object data;

    public SagaEvent() {}

    public SagaEvent(String sagaId, String step, String status, Object data) {
        this.sagaId = sagaId;
        this.step = step;
        this.status = status;
        this.data = data;
    }

    public String getSagaId() {
        return sagaId;
    }

    public String getStep() {
        return step;
    }

    public String getStatus() {
        return status;
    }

    public Object getData() {
        return data;
    }

    public void setSagaId(String sagaId) {
        this.sagaId = sagaId;
    }

    public void setStep(String step) {
        this.step = step;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
