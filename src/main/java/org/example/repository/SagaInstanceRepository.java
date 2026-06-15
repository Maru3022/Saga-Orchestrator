package org.example.repository;

import org.example.model.SagaInstance;
import org.example.model.SagaStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface SagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {

    Optional<SagaInstance> findByCorrelationId(String correlationId);

    boolean existsByCorrelationId(String correlationId);

    List<SagaInstance> findByStatus(SagaStatus status);

    List<SagaInstance> findByUserId(String userId);
}
