package org.example.temporal;

import java.util.HashMap;
import java.util.Map;

public class CreateProgramInput {
    private String sagaId;
    private String ownerId;
    private Map<String, Object> payload = new HashMap<>();

    public CreateProgramInput() {}

    public CreateProgramInput(String sagaId, String ownerId, Map<String, Object> payload) {
        this.sagaId = sagaId;
        this.ownerId = ownerId;
        this.payload = payload == null ? new HashMap<>() : payload;
    }

    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }
    public String getOwnerId() { return ownerId; }
    public void setOwnerId(String ownerId) { this.ownerId = ownerId; }
    public Map<String, Object> getPayload() { return payload; }
    public void setPayload(Map<String, Object> payload) { this.payload = payload; }
}
