package org.example.temporal;

import java.util.Locale;

public final class TemporalCommandIds {
    private TemporalCommandIds() {}

    public static String forStep(String sagaId, String step, String operation) {
        String commandStep = "TRAINING".equalsIgnoreCase(step) || "TRAIN".equalsIgnoreCase(step)
                ? "TRAINS"
                : step.toUpperCase(Locale.ROOT);
        String commandOperation = "COMPENSATE".equalsIgnoreCase(operation)
                ? "ROLLBACK"
                : "EXECUTE";
        return sagaId + ":" + commandStep + ":" + commandOperation;
    }
}
