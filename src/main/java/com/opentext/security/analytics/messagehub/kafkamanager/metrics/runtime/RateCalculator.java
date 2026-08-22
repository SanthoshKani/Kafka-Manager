package com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Pure per-minute counter rate calculator.
 * <p>Usage: call {@link #calculate(MetricSample, MetricSample)} with two counter samples.
 */
public final class RateCalculator {

    /**
     * Maximum allowed interval (seconds) between samples to compute a rate. If <=0 no limit is enforced.
     */
    private final long maxIntervalSeconds;

    public RateCalculator() {
        this(0);
    }

    public RateCalculator(long maxIntervalSeconds) {
        if (maxIntervalSeconds < 0) throw new IllegalArgumentException("maxIntervalSeconds must be >= 0");
        this.maxIntervalSeconds = maxIntervalSeconds;
    }

    public RateResult calculate(MetricSample previous, MetricSample current) {
        if (previous == null) return RateResult.InsufficientData.INSTANCE;
        Objects.requireNonNull(current, "current");

        // Semantic type must be a counter
        if (previous.semanticType() != MetricSemanticType.MONOTONIC_COUNTER
                || current.semanticType() != MetricSemanticType.MONOTONIC_COUNTER) {
            return new RateResult.InvalidSample("semanticType must be MONOTONIC_COUNTER");
        }

        Instant tPrev = previous.timestamp();
        Instant tCurr = current.timestamp();
        if (tCurr.isBefore(tPrev)) {
            return new RateResult.InvalidSample("current timestamp is before previous timestamp");
        }
        long elapsedSeconds = Duration.between(tPrev, tCurr).getSeconds();
        if (elapsedSeconds <= 0) {
            return RateResult.InsufficientData.INSTANCE; // duplicate timestamp or zero interval
        }

        if (maxIntervalSeconds > 0 && elapsedSeconds > maxIntervalSeconds) {
            return RateResult.InsufficientData.INSTANCE; // configuration: treat long gaps as insufficient
        }

        double delta = current.value() - previous.value();
        if (delta < 0) {
            // Counter reset
            return new RateResult.CounterReset();
        }

        double ratePerMinute = delta / ((double) elapsedSeconds) * 60.0;
        return new RateResult.ValidRate(ratePerMinute, delta, elapsedSeconds);
    }

    /**
     * Sealed result type for rate calculations.
     */
    public static sealed interface RateResult {
        record ValidRate(double ratePerMinute, double delta, double elapsedSeconds) implements RateResult {}

        enum InsufficientData implements RateResult {
            INSTANCE
        }

        final class CounterReset implements RateResult {
            @Override
            public String toString() {
                return "CounterReset";
            }
        }

        final class InvalidSample implements RateResult {
            private final String reason;

            public InvalidSample(String reason) {
                this.reason = reason;
            }

            public String reason() {
                return reason;
            }

            @Override
            public String toString() {
                return "InvalidSample(" + reason + ")";
            }
        }
    }
}
