package org.example.temporal;

import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.testing.TestActivityEnvironment;
import org.example.model.SagaEvent;
import org.example.service.handlers.StepHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class CreateProgramActivitiesImplTest {

    private TestActivityEnvironment environment;
    private StepHandler trains;
    private CreateProgramActivities activities;

    @BeforeEach
    void setUp() {
        environment = TestActivityEnvironment.newInstance();
        trains = mock(StepHandler.class);
        when(trains.getStepName()).thenReturn("TRAINS");
        environment.registerActivitiesImplementations(
                new CreateProgramActivitiesImpl(List.of(trains)));
        activities = environment.newActivityStub(CreateProgramActivities.class);
    }

    @AfterEach
    void tearDown() {
        environment.close();
    }

    @Test
    void executeUsesTrainsHandlerAndCarriesVersionedCommandIdentity() {
        activities.executeStep(input(), "TRAINING");

        ArgumentCaptor<SagaEvent> captor = ArgumentCaptor.forClass(SagaEvent.class);
        verify(trains).processForward(captor.capture());
        SagaEvent command = captor.getValue();
        assertEquals("temporal-operation-1:TRAINS:EXECUTE", command.getCommandId());
        assertEquals("EXECUTE", command.getOperation());
        assertEquals(1, command.getEventVersion());
        assertEquals(Map.of("userId", "owner-1"), command.getData());
    }

    @Test
    void compensationUsesStableCompensationCommandIdentity() {
        activities.compensateStep(input(), "TRAINING");

        ArgumentCaptor<SagaEvent> captor = ArgumentCaptor.forClass(SagaEvent.class);
        verify(trains).processRollback(captor.capture());
        SagaEvent command = captor.getValue();
        assertEquals("temporal-operation-1:TRAINS:ROLLBACK", command.getCommandId());
        assertEquals("COMPENSATE", command.getOperation());
        assertEquals(1, command.getEventVersion());
    }

    @Test
    void rejectsUnknownStepBeforeAnyDownstreamInvocation() {
        ActivityFailure failure = assertThrows(
                ActivityFailure.class,
                () -> activities.executeStep(input(), "UNKNOWN"));
        ApplicationFailure cause = assertInstanceOf(ApplicationFailure.class, failure.getCause());
        assertEquals("Unknown step: UNKNOWN", cause.getOriginalMessage());
        verify(trains, never()).processForward(any());
        verify(trains, never()).processRollback(any());
    }

    private CreateProgramInput input() {
        return new CreateProgramInput(
                "temporal-operation-1", "owner-1", Map.of("userId", "owner-1"));
    }
}
