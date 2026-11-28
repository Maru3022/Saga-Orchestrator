package org.example.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.model.OutboxEvent;
import org.example.model.SagaInstance;
import org.example.model.SagaState;
import org.example.model.SagaStatus;
import org.example.model.SagaStep;
import org.example.repository.OutboxEventRepository;
import org.example.repository.SagaInstanceRepository;
import org.example.service.SagaOutboxService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
@SpringBootTest(properties = {
        "spring.cloud.discovery.enabled=false",
        "eureka.client.enabled=false",
        "spring.kafka.bootstrap-servers=localhost:9092"
})
class OutboxTransactionIT {

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("saga_orchestrator")
                    .withUsername("fitflow")
                    .withPassword("fitflow-test-placeholder");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.flyway.enabled", () -> "true");
    }

    @Autowired
    private SagaOutboxService outboxService;

    @Autowired
    private OutboxEventRepository outboxRepository;

    @Autowired
    private SagaInstanceRepository sagaInstanceRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void cleanDatabase() {
        outboxRepository.deleteAll();
        sagaInstanceRepository.deleteAll();
    }

    @Test
    void commitsSagaInstanceAndOutboxEventTogether() {
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        OutboxEvent event = template.execute(status -> {
            SagaInstance instance = sagaInstance();
            sagaInstanceRepository.save(instance);
            return outboxService.createForSagaInstance(
                    "CREATE_PROGRAM_REQUESTED",
                    instance,
                    sagaState());
        });

        assertNotNull(event);
        assertEquals(1L, outboxRepository.count());
        assertEquals("CREATE_PROGRAM_REQUESTED",
                outboxRepository.findById(event.getId()).orElseThrow().getEventType());
    }

    @Test
    void rollsBackOutboxAndBusinessRecordTogether() {
        long outboxBefore = outboxRepository.count();
        TransactionTemplate template = new TransactionTemplate(transactionManager);

        assertThrows(IllegalStateException.class, () ->
                template.execute(status -> {
                    SagaInstance instance = sagaInstance();
                    sagaInstanceRepository.save(instance);
                    outboxService.createForSagaInstance(
                            "CREATE_PROGRAM_REQUESTED",
                            instance,
                            sagaState());
                    throw new IllegalStateException("forced rollback");
                }));

        assertEquals(outboxBefore, outboxRepository.count());
        assertEquals(0, sagaInstanceRepository.count());
    }

    @Test
    void flywayCreatedOutboxJsonbTableAndPayloadIsValidJson() throws Exception {
        assertEquals(1, jdbcTemplate.queryForObject(
                "select count(*) from information_schema.tables " +
                        "where table_schema = 'public' and table_name = 'outbox_events'",
                Integer.class));

        SagaState state = sagaState();
        String payload = new ObjectMapper().writeValueAsString(state);
        assertEquals(true, jdbcTemplate.queryForObject(
                "select ?::jsonb is not null",
                Boolean.class,
                payload));
    }

    private SagaInstance sagaInstance() {
        return SagaInstance.builder()
                .id(UUID.randomUUID())
                .correlationId(UUID.randomUUID().toString())
                .userId("owner-1")
                .userEmail("owner@example.invalid")
                .sagaPayload("{}")
                .state(SagaStatus.STARTED)
                .currentStep(SagaStep.CREATE_USER)
                .retryCount(0)
                .maxRetries(3)
                .build();
    }

    private SagaState sagaState() {
        SagaState state = new SagaState("CREATE_PROGRAM", Map.of("goal", "fit"));
        state.setOwnerId("owner-1");
        return state;
    }
}
