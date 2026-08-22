package com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime;

import java.time.Instant;
import java.util.Objects;

/**
 * A normalized runtime metric sample.
 */
public record MetricSample(
        MetricIdentity identity,
        double value,
        Instant timestamp,
        MetricSemanticType semanticType,
        String unit,
        MetricSourceBackend sourceBackend,
        String sourceAttributeName) {

    public MetricSample {
        identity = Objects.requireNonNull(identity, "identity");
        timestamp = Objects.requireNonNull(timestamp, "timestamp");
        semanticType = Objects.requireNonNull(semanticType, "semanticType");
        unit = Objects.requireNonNull(unit, "unit");
        sourceBackend = Objects.requireNonNull(sourceBackend, "sourceBackend");

        // Validate numeric value: reject NaN and infinite
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("metric value must be a finite number");
        }
    }
}
