package org.example.model;

import java.io.Serializable;
import java.util.Map;

public class SagaCommandEvent implements Serializable {

    private String eventId;
    private String sagaId;
    private String step;
    private String status;
    private Map<String, Object> data;

    public SagaCommandEvent() {}

    public SagaCommandEvent(String eventId, String sagaId, String step,
                            String status, Map<String, Object> data) {
        this.eventId = eventId;
        this.sagaId = sagaId;
        this.step = step;
        this.status = status;
        this.data = data;
    }

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
}