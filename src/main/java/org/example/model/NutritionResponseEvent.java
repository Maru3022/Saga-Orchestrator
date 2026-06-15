package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NutritionResponseEvent {
    private String correlationId;
    private String userId;
    private boolean success;
    private String errorMessage;
}
