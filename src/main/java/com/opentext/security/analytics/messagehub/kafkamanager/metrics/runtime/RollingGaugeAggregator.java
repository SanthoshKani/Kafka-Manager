package com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Rolling aggregator for gauge samples.
 *
 * <p>This aggregator computes:
 * - latest value
 * - time-weighted average over the configured window (documented as time-weighted)
 * - minimum
 * - maximum
 * - sample count
 * - freshness (seconds since last sample)
 *
 * Behavior and assumptions:
 * - The average is time-weighted: each sample value is assumed to persist until the next sample.
 * - A valid aggregation requires a sample at or before the window start (so the window is fully covered).
 * - Non-finite sample values (NaN/Infinite) cause an InvalidSample result.
 * - If coverage is incomplete (no sample at or before window start), InsufficientData is returned.
 */
public final class RollingGaugeAggregator {

    private final Duration window;

    public RollingGaugeAggregator(Duration window) {
        this.window = Objects.requireNonNull(window, "window");
        if (window.compareTo(Duration.ZERO) <= 0) throw new IllegalArgumentException("window must be > 0");
    }

    public AggregationResult aggregate(List<MetricSample> samples, Instant now) {
        Objects.requireNonNull(samples, "samples");
        Objects.requireNonNull(now, "now");

        if (samples.isEmpty()) return AggregationResult.InsufficientData.INSTANCE;

        // sort by timestamp ascending
        List<MetricSample> sorted = samples.stream()
                .sorted(java.util.Comparator.comparing(MetricSample::timestamp))
                .toList();
        Instant windowEnd = now;
        Instant windowStart = now.minus(window);

        // Check for non-finite values
        for (MetricSample s : sorted) {
            if (!Double.isFinite(s.value())) {
                return new AggregationResult.InvalidSample("non-finite sample value");
            }
            if (s.semanticType() != MetricSemanticType.GAUGE) {
                return new AggregationResult.InvalidSample("semanticType must be GAUGE");
            }
        }

        // find the sample at or before windowStart
        MetricSample initial = null;
        for (int i = 0; i < sorted.size(); i++) {
            MetricSample s = sorted.get(i);
            if (!s.timestamp().isAfter(windowStart)) {
                initial = s;
            } else {
                break;
            }
        }
        if (initial == null) {
            return AggregationResult.InsufficientData.INSTANCE; // cannot cover start of window
        }

        // compute time-weighted average by walking samples from initial onwards
        double weightedSum = 0.0;
        long totalSeconds = window.getSeconds();
        Instant cursor = windowStart;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        int count = 0;

        // Start with initial value
        double value = initial.value();
        min = Math.min(min, value);
        max = Math.max(max, value);
        count = 1; // include initial

        // iterate over samples with timestamp > windowStart and <= windowEnd
        for (MetricSample s : sorted) {
            if (!s.timestamp().isAfter(windowStart)) continue; // skip until after windowStart
            if (s.timestamp().isAfter(windowEnd)) break;
            Instant sampleTime = s.timestamp();
            long segment = Duration.between(cursor, sampleTime).getSeconds();
            if (segment < 0) return new AggregationResult.InvalidSample("out-of-order timestamps");
            weightedSum += value * segment;
            // advance
            cursor = sampleTime;
            value = s.value();
            min = Math.min(min, value);
            max = Math.max(max, value);
            count++;
        }

        // final segment from cursor to windowEnd
        long finalSeg = Duration.between(cursor, windowEnd).getSeconds();
        if (finalSeg < 0) return new AggregationResult.InvalidSample("timestamps after window end");
        weightedSum += value * finalSeg;

        double avg = weightedSum / (double) totalSeconds;

        // latest sample is the last sample <= windowEnd
        MetricSample latest = null;
        for (int i = sorted.size() - 1; i >= 0; i--) {
            MetricSample s = sorted.get(i);
            if (!s.timestamp().isAfter(windowEnd)) {
                latest = s;
                break;
            }
        }
        if (latest == null) return AggregationResult.InsufficientData.INSTANCE;

        long freshnessSeconds = Duration.between(latest.timestamp(), windowEnd).getSeconds();

        return new AggregationResult.Valid(
                latest.value(),
                avg,
                min == Double.POSITIVE_INFINITY ? Double.NaN : min,
                max == Double.NEGATIVE_INFINITY ? Double.NaN : max,
                count,
                freshnessSeconds);
    }

    public static sealed interface AggregationResult {
        record Valid(double latest, double average, double min, double max, int count, long freshnessSeconds)
                implements AggregationResult {}

        enum InsufficientData implements AggregationResult {
            INSTANCE
        }

        final class InvalidSample implements AggregationResult {
            private final String reason;

            public InvalidSample(String reason) {
                this.reason = reason;
            }

            public String reason() {
                return reason;
            }
        }
    }
}
