package org.example.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserCreatedEvent {
    @JsonAlias("eventId")
    private String correlationId;
    private String userId;
    private String username;
    private String email;
    private String fullName;
    private boolean success;
    private String errorMessage;
}
