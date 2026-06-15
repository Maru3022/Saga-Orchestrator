package org.example.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "saga_instances", indexes = {
        @Index(name = "idx_correlation_id", columnList = "correlation_id", unique = true),
        @Index(name = "idx_saga_status", columnList = "status"),
        @Index(name = "idx_user_id", columnList = "user_id")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaInstance {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "correlation_id", unique = true, nullable = false, length = 36)
    private String correlationId;

    @Column(name = "user_id", nullable = false, length = 36)
    private String userId;

    @Column(name = "user_email", nullable = false, length = 255)
    private String userEmail;

    @Column(name = "saga_payload", columnDefinition = "TEXT")
    private String sagaPayload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 50)
    private SagaStatus state;

    @Enumerated(EnumType.STRING)
    @Column(name = "current_step", nullable = false, length = 50)
    private SagaStep currentStep;

    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private Integer retryCount = 0;

    @Column(name = "max_retries", nullable = false)
    @Builder.Default
    private Integer maxRetries = 3;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @PrePersist
    protected void onCreate() {
        if (this.id == null) {
            this.id = UUID.randomUUID();
        }
        if (this.correlationId == null) {
            this.correlationId = UUID.randomUUID().toString();
        }
        if (this.state == null) {
            this.state = SagaStatus.STARTED;
        }
        if (this.currentStep == null) {
            this.currentStep = SagaStep.CREATE_USER;
        }
    }
}
