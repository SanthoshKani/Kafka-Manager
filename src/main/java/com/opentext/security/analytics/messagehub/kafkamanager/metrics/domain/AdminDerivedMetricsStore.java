package com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * Repository-style interface for storing the latest Admin-derived metrics snapshot per cluster.
 */
public interface AdminDerivedMetricsStore {

    /**
     * Return the current snapshot for the cluster. Never returns null; an empty snapshot is used when
     * no data exists.
     */
    AdminClientMetricsSnapshot getCurrent(UUID clusterId);

    /**
     * Save a successful snapshot. Implementations should update lastSuccessfulCollectionAt accordingly.
     */
    void saveSuccessful(AdminClientMetricsSnapshot snapshot);

    /**
     * Record a failed collection attempt for the cluster without clearing previous successful values.
     * The attempt timestamp and sanitized failure reason should be recorded.
     */
    void recordFailure(UUID clusterId, Instant attemptAt, String sanitizedFailureReason);

    /**
     * Remove any stored snapshot for the cluster.
     */
    void remove(UUID clusterId);

    /**
     * Return true when the store contains any entry for the cluster (successful or failed attempts).
     */
    default boolean exists(UUID clusterId) {
        // Default implementation assumes store will override for optimized lookup.
        try {
            return getCurrent(clusterId) != null && getCurrent(clusterId).collectionStatus() != null;
        } catch (RuntimeException e) {
            return false;
        }
    }
}
