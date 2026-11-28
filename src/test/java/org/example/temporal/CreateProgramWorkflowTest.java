package org.example.temporal;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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
    void acceptsDuplicateSignalsAndSignalDeliveredBeforeOutcomeWait() {
        activities.duplicateSignals = true;
        CreateProgramWorkflow workflow = newWorkflow("duplicate-signals");

        assertEquals("COMPLETED", workflow.createProgram(input("duplicate-signals")));
        assertEquals(List.of("TRAINING", "NUTRITION", "NOTIFICATION"), activities.executed);
        assertTrue(activities.compensated.isEmpty());
    }

    @Test
    void compensatesCompletedStepsAfterTemporaryFailure() {
        activities.failOutcomeStep = "NUTRITION";
        CreateProgramWorkflow workflow = newWorkflow("temporary-failure");

        assertThrows(WorkflowFailedException.class,
                () -> workflow.createProgram(input("temporary-failure")));
        assertEquals(List.of("TRAINING"), activities.compensated);
    }

    @Test
    void retriesTemporaryFailureThenSucceeds() {
        activities.temporaryFailureThenSuccessStep = "NUTRITION";
        CreateProgramWorkflow workflow = newWorkflow("retry-success");

        assertEquals("COMPLETED", workflow.createProgram(input("retry-success")));
        assertEquals(
                List.of("TRAINING", "NUTRITION", "NOTIFICATION"),
                activities.executed);
        assertEquals(2, activities.temporaryAttempt);
    }

    @Test
    void compensationContinuesWhenOneCompensationFails() {
        activities.failOutcomeStep = "NOTIFICATION";
        activities.failCompensationStep = "NUTRITION";
        CreateProgramWorkflow workflow = newWorkflow("compensation-failure");

        assertThrows(WorkflowFailedException.class,
                () -> workflow.createProgram(input("compensation-failure")));
        assertTrue(activities.compensated.contains("NUTRITION"));
        assertTrue(activities.compensated.contains("TRAINING"));
    }

    @Test
    void timesOutDeterministicallyAndCompensatesAnAmbiguousStep() {
        activities.suppressOutcomeStep = "NUTRITION";
        CreateProgramWorkflow workflow = newWorkflow("outcome-timeout");

        assertThrows(WorkflowFailedException.class,
                () -> workflow.createProgram(input("outcome-timeout")));
        assertEquals(List.of("NUTRITION", "TRAINING"), activities.compensated);
    }

    @Test
    void cancellationReachesCompensationPath() {
        activities.blockStep = "NUTRITION";
        CreateProgramWorkflow workflow = newWorkflow("cancel");
        WorkflowStub stub = WorkflowStub.fromTyped(workflow);
        stub.start(input("cancel"));
        try {
            assertTrue(activities.blockEntered.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for the workflow activity", e);
        }
        stub.cancel();
        activities.releaseBlockedStep.countDown();
        assertThrows(Exception.class, () -> stub.getResult(String.class));
        assertEquals(List.of("NUTRITION", "TRAINING"), activities.compensated);
    }

    private CreateProgramWorkflow newWorkflow(String id) {
        String workflowId = "fitflow-create-program-" + id;
        activities.signalReceiver = outcome -> workflowClient
                .newWorkflowStub(CreateProgramWorkflow.class, workflowId)
                .receiveOutcome(outcome);
        return workflowClient.newWorkflowStub(
                CreateProgramWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(CreateProgramWorkflowImpl.TASK_QUEUE)
                        .setWorkflowId(workflowId)
                        .build());
    }

    private CreateProgramInput input(String id) {
        return new CreateProgramInput(id, "owner-1", Map.of("goal", "fitness"));
    }

    private static class RecordingActivities implements CreateProgramActivities {
        private final List<String> executed = new ArrayList<>();
        private final List<String> compensated = new ArrayList<>();
        private String failOutcomeStep;
        private String temporaryFailureThenSuccessStep;
        private String failCompensationStep;
        private int temporaryAttempt;
        private String blockStep;
        private String suppressOutcomeStep;
        private Consumer<StepOutcomeSignal> signalReceiver;
        private final CountDownLatch blockEntered = new CountDownLatch(1);
        private final CountDownLatch releaseBlockedStep = new CountDownLatch(1);
        private final AtomicInteger eventSequence = new AtomicInteger();

        @Override
        public void executeStep(CreateProgramInput input, String stepName) {
            if (stepName.equals(temporaryFailureThenSuccessStep) && temporaryAttempt++ == 0) {
                throw new IllegalStateException("Transient failure");
            }
            executed.add(stepName);
            awaitReleaseIfBlocked(stepName);
            boolean success = !stepName.equals(failOutcomeStep);
            if (!stepName.equals(suppressOutcomeStep)) {
                signalReceiver.accept(signal(input, stepName, "EXECUTE",
                        success ? "SUCCESS" : "FAILED"));
                if (duplicateSignals) {
                    signalReceiver.accept(signal(input, stepName, "EXECUTE",
                            success ? "SUCCESS" : "FAILED"));
                }
            }
        }

        @Override
        public void compensateStep(CreateProgramInput input, String stepName) {
            compensated.add(stepName);
            boolean success = !stepName.equals(failCompensationStep);
            signalReceiver.accept(signal(input, stepName, "COMPENSATE",
                    success ? "ROLLBACK_DONE" : "FAILED"));
        }

        private boolean duplicateSignals;

        private StepOutcomeSignal signal(CreateProgramInput input, String stepName,
                                         String operation, String status) {
            return new StepOutcomeSignal(
                    "response-" + eventSequence.incrementAndGet(),
                    input.getSagaId(),
                    stepName,
                    operation,
                    TemporalCommandIds.forStep(input.getSagaId(), stepName, operation),
                    status);
        }

        private void awaitReleaseIfBlocked(String stepName) {
            if (!stepName.equals(blockStep)) {
                return;
            }
            blockEntered.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    releaseBlockedStep.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
