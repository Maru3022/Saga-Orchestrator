package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CabinetResponseEvent {
    private String correlationId;
    private String userId;
    private String cabinetId;
    private boolean success;
    private String errorMessage;
}
