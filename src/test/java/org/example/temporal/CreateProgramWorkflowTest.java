package org.example.temporal;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CreateProgramWorkflowTest {

    private TestWorkflowEnvironment environment;
    private RecordingActivities activities;
    private WorkflowClient workflowClient;

    @BeforeEach
    void setUp() {
        environment = TestWorkflowEnvironment.newInstance(
                TestEnvironmentOptions.newBuilder().build());
        activities = new RecordingActivities();
        Worker worker = environment.newWorker(CreateProgramWorkflowImpl.TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(CreateProgramWorkflowImpl.class);
        worker.registerActivitiesImplementations(activities);
        environment.start();
        workflowClient = environment.getWorkflowClient();
    }

    @AfterEach
    void tearDown() {
        environment.close();
    }

    @Test
    void completesAllStepsAndPersistsStatus() {
        CreateProgramWorkflow workflow = newWorkflow("success");

        assertEquals("COMPLETED", workflow.createProgram(input("success")));
        assertEquals(List.of("TRAINING", "NUTRITION", "NOTIFICATION"), activities.executed);
        assertTrue(activities.compensated.isEmpty());
    }

    @Test
    void compensatesCompletedStepsAfterTemporaryFailure() {
        activities.failOnStep = "NUTRITION";
        CreateProgramWorkflow workflow = newWorkflow("temporary-failure");

        assertThrows(WorkflowFailedException.class,
                () -> workflow.createProgram(input("temporary-failure")));
        assertEquals(List.of("TRAINING"), activities.compensated);
    }

    private CreateProgramWorkflow newWorkflow(String id) {
        return workflowClient.newWorkflowStub(
                CreateProgramWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(CreateProgramWorkflowImpl.TASK_QUEUE)
                        .setWorkflowId("fitflow-create-program-" + id)
                        .build());
    }

    private CreateProgramInput input(String id) {
        return new CreateProgramInput(id, "owner-1", Map.of("goal", "fitness"));
    }

    private static class RecordingActivities implements CreateProgramActivities {
        private final List<String> executed = new ArrayList<>();
        private final List<String> compensated = new ArrayList<>();
        private String failOnStep;

        @Override
        public void executeStep(CreateProgramInput input, String stepName) {
            executed.add(stepName);
            if (stepName.equals(failOnStep)) {
                throw new IllegalStateException("Temporary activity failure");
            }
        }

        @Override
        public void compensateStep(CreateProgramInput input, String stepName) {
            compensated.add(stepName);
        }
    }
}
