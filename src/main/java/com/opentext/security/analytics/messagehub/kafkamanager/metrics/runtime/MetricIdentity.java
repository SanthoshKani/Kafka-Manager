package com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime;

import java.util.Objects;
import java.util.UUID;

/**
 * Canonical identity for a runtime metric.
 * Fields are normalized in the canonical constructor to ensure deterministic equality.
 */
public record MetricIdentity(UUID clusterId, Integer brokerId, String topic, String metricName) {

    public MetricIdentity {
        clusterId = Objects.requireNonNull(clusterId, "clusterId");
        // brokerId may be null for cluster-scoped metrics
        if (brokerId != null && brokerId < 0) {
            throw new IllegalArgumentException("brokerId must be >= 0");
        }
        // Normalize topic: empty or blank -> null; otherwise lowercase for deterministic identity
        if (topic != null) {
            topic = topic.trim();
            if (topic.isBlank()) {
                topic = null;
            } else {
                topic = topic.toLowerCase();
            }
        }
        // metricName is required and normalized to lower-case to ensure deterministic identity
        metricName = Objects.requireNonNull(metricName, "metricName").trim().toLowerCase();
        if (metricName.isEmpty()) {
            throw new IllegalArgumentException("metricName must not be empty");
        }
    }

    // topic accessor returns the canonical topic string or null when absent
}
