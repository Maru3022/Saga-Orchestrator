package org.example.model;

public class UserCreatedEvent {
    private String eventId;
    private String userId;
    private String username;
    private String email;
    private String fullName;
    private long occurredAt;

    public UserCreatedEvent() {}

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public String getFullName() { return fullName; }
    public void setFullName(String fullName) { this.fullName = fullName; }

    public long getOccurredAt() { return occurredAt; }
    public void setOccurredAt(long occurredAt) { this.occurredAt = occurredAt; }
}