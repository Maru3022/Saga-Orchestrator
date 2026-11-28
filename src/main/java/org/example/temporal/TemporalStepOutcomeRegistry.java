package org.example.temporal;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class TemporalStepOutcomeRegistry {

    private final Map<String, PendingOutcome> pending = new ConcurrentHashMap<>();

    public StepOutcome await(String sagaId, String stepName, long timeoutMillis) {
        String key = key(sagaId, stepName);
        PendingOutcome outcome = pending.computeIfAbsent(key, ignored -> new PendingOutcome());
        try {
            boolean received = outcome.latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
            if (!received) {
                throw new IllegalStateException("Timed out waiting for downstream step: " + stepName);
            }
            return outcome.result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for downstream step: " + stepName, e);
        } finally {
            pending.remove(key, outcome);
        }
    }

    public boolean complete(String sagaId, String stepName, StepOutcome outcome) {
        PendingOutcome pendingOutcome = pending.get(key(sagaId, stepName));
        if (pendingOutcome == null) {
            return false;
        }
        pendingOutcome.result.set(outcome);
        pendingOutcome.latch.countDown();
        return true;
    }

    public void cancel(String sagaId, String stepName) {
        pending.remove(key(sagaId, stepName));
    }

    private static String key(String sagaId, String stepName) {
        return sagaId + ":" + stepName;
    }

    private static class PendingOutcome {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<StepOutcome> result = new AtomicReference<>();
    }
}
