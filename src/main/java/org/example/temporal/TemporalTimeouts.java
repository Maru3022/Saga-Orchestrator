package org.example.temporal;

import java.time.Duration;

public final class TemporalTimeouts {

    public static final Duration ACTIVITY_START_TO_CLOSE = Duration.ofSeconds(30);
    public static final Duration ACTIVITY_SCHEDULE_TO_CLOSE = Duration.ofMinutes(3);
    public static final Duration OUTCOME_TIMEOUT = Duration.ofMinutes(2);
    public static final Duration OUTCOME_RECOVERY_TIMEOUT = Duration.ofMinutes(2);
    public static final Duration WORKFLOW_EXECUTION_TIMEOUT = Duration.ofMinutes(45);

    private TemporalTimeouts() {}
}
