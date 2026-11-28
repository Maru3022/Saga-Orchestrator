package org.example.temporal;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
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

        assertEquals("FAILED", workflow.createProgram(input("temporary-failure")));
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
    void doesNotCompensateAStepWhoseActivityNeverSucceeded() {
        activities.failActivityStep = "NUTRITION";
        CreateProgramWorkflow workflow = newWorkflow("activity-failure");

        assertEquals("UNKNOWN", workflow.createProgram(input("activity-failure")));
        assertEquals(List.of("TRAINING"), activities.executed);
        assertTrue(activities.compensated.isEmpty());
    }

    @Test
    void compensationContinuesWhenOneCompensationFails() {
        activities.failOutcomeStep = "NOTIFICATION";
        activities.failCompensationStep = "NUTRITION";
        CreateProgramWorkflow workflow = newWorkflow("compensation-failure");

        assertEquals("COMPENSATION_FAILED",
                workflow.createProgram(input("compensation-failure")));
        assertTrue(activities.compensated.contains("NUTRITION"));
        assertTrue(activities.compensated.contains("TRAINING"));
    }

    @Test
    void leavesTimedOutStepUnknownWithoutStartingAnyCompensation() {
        activities.suppressOutcomeStep = "NUTRITION";
        CreateProgramWorkflow workflow = newWorkflow("outcome-timeout");

        assertEquals("UNKNOWN", workflow.createProgram(input("outcome-timeout")));
        assertTrue(activities.compensated.isEmpty());
    }

    @Test
    void acceptsSuccessAfterTheInitialOutcomeTimeout() {
        activities.lateOutcomeStep = "NUTRITION";
        CreateProgramWorkflow workflow = newWorkflow("late-success");
        environment.registerDelayedCallback(
                Duration.ofMinutes(2).plusSeconds(30),
                () -> assertEquals(
                        StepState.UNKNOWN,
                        workflow.getStepStates().get("NUTRITION:EXECUTE")));

        assertEquals("COMPLETED", workflow.createProgram(input("late-success")));
        assertTrue(activities.compensated.isEmpty());
    }

    @Test
    void doesNotCompensateWhenProducerAckIsLostAfterDownstreamSuccess() {
        activities.publishThenFailStep = "NUTRITION";
        CreateProgramWorkflow workflow = newWorkflow("ack-lost");

        assertEquals("COMPLETED", workflow.createProgram(input("ack-lost")));
        assertTrue(activities.compensated.isEmpty());
    }

    @Test
    void stopsCompensationWhenItsOutcomeIsUnknown() {
        activities.failOutcomeStep = "NOTIFICATION";
        activities.suppressCompensationOutcomeStep = "NUTRITION";
        CreateProgramWorkflow workflow = newWorkflow("compensation-unknown");

        assertEquals("UNKNOWN", workflow.createProgram(input("compensation-unknown")));
        assertEquals(List.of("NUTRITION"), activities.compensated);
    }

    @Test
    void cancellationDoesNotCompensateAnUnknownOperation() {
        activities.blockStep = "NUTRITION";
        activities.suppressOutcomeStep = "NUTRITION";
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
        assertTrue(activities.compensated.isEmpty());
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
                        .setWorkflowExecutionTimeout(
                                TemporalTimeouts.WORKFLOW_EXECUTION_TIMEOUT)
                        .build());
    }

    private CreateProgramInput input(String id) {
        return new CreateProgramInput(id, "owner-1", Map.of("goal", "fitness"));
    }

    private class RecordingActivities implements CreateProgramActivities {
        private final List<String> executed = new ArrayList<>();
        private final List<String> compensated = new ArrayList<>();
        private String failOutcomeStep;
        private String failActivityStep;
        private String temporaryFailureThenSuccessStep;
        private String failCompensationStep;
        private String lateOutcomeStep;
        private String publishThenFailStep;
        private String suppressCompensationOutcomeStep;
        private int temporaryAttempt;
        private String blockStep;
        private String suppressOutcomeStep;
        private Consumer<StepOutcomeSignal> signalReceiver;
        private final CountDownLatch blockEntered = new CountDownLatch(1);
        private final CountDownLatch releaseBlockedStep = new CountDownLatch(1);
        private final AtomicInteger eventSequence = new AtomicInteger();

        @Override
        public void executeStep(CreateProgramInput input, String stepName) {
            if (stepName.equals(failActivityStep)) {
                throw new IllegalStateException("Permanent activity failure");
            }
            if (stepName.equals(temporaryFailureThenSuccessStep) && temporaryAttempt++ == 0) {
                throw new IllegalStateException("Transient failure");
            }
            executed.add(stepName);
            awaitReleaseIfBlocked(stepName);
            boolean success = !stepName.equals(failOutcomeStep);
            if (stepName.equals(publishThenFailStep)) {
                signalReceiver.accept(signal(input, stepName, "EXECUTE", "SUCCESS"));
                throw new IllegalStateException("Producer acknowledgement was lost");
            }
            if (stepName.equals(lateOutcomeStep)) {
                environment.registerDelayedCallback(
                        Duration.ofMinutes(3),
                        () -> signalReceiver.accept(
                                signal(input, stepName, "EXECUTE", "SUCCESS")));
            } else if (!stepName.equals(suppressOutcomeStep)) {
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
            if (!stepName.equals(suppressCompensationOutcomeStep)) {
                signalReceiver.accept(signal(input, stepName, "COMPENSATE",
                        success ? "ROLLBACK_DONE" : "FAILED"));
            }
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
