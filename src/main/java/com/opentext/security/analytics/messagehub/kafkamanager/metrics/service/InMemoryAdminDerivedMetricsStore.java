package com.opentext.security.analytics.messagehub.kafkamanager.metrics.service;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminDerivedMetricsStore;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Simple concurrency-safe in-memory store keeping at most one current snapshot per cluster.
 *
 * <p>On failures this implementation preserves the last successful metric values and only updates
 * last-attempt timestamp, collection status and sanitized failure reason.
 */
@Component
@ConditionalOnProperty(prefix = "app.features", name = "metrics.enabled", havingValue = "true", matchIfMissing = false)
@Primary
public class InMemoryAdminDerivedMetricsStore implements AdminDerivedMetricsStore {

    private final ConcurrentMap<UUID, AtomicReference<AdminClientMetricsSnapshot>> map = new ConcurrentHashMap<>();

    @Override
    public AdminClientMetricsSnapshot getCurrent(UUID clusterId) {
        AtomicReference<AdminClientMetricsSnapshot> ref = map.get(clusterId);
        return ref == null ? emptySnapshot(clusterId) : ref.get();
    }

    @Override
    public void saveSuccessful(AdminClientMetricsSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        UUID clusterId = snapshot.clusterId();
        map.compute(clusterId, (id, existingRef) -> {
            if (existingRef == null) {
                // Ensure lastSuccessfulCollectionAt is set to collectedAt when success
                AdminClientMetricsSnapshot normalized = new AdminClientMetricsSnapshot(
                        snapshot.clusterId(),
                        snapshot.collectedAt(),
                        snapshot.brokerCount(),
                        snapshot.topicCount(),
                        snapshot.partitionCount(),
                        snapshot.offlinePartitionCount(),
                        snapshot.underReplicatedPartitionCount(),
                        snapshot.controllerBrokerId(),
                        CollectionStatus.SUCCESS,
                        snapshot.collectedAt(),
                        null,
                        snapshot.brokerLeaderCounts());
                return new AtomicReference<>(normalized);
            } else {
                // Replace with successful snapshot; ensure lastSuccessfulCollectionAt updated
                AdminClientMetricsSnapshot prev = existingRef.get();
                AdminClientMetricsSnapshot normalized = new AdminClientMetricsSnapshot(
                        snapshot.clusterId(),
                        snapshot.collectedAt(),
                        snapshot.brokerCount(),
                        snapshot.topicCount(),
                        snapshot.partitionCount(),
                        snapshot.offlinePartitionCount(),
                        snapshot.underReplicatedPartitionCount(),
                        snapshot.controllerBrokerId(),
                        CollectionStatus.SUCCESS,
                        snapshot.collectedAt(),
                        null,
                        snapshot.brokerLeaderCounts());
                existingRef.set(normalized);
                return existingRef;
            }
        });
    }

    @Override
    public void recordFailure(UUID clusterId, Instant attemptAt, String sanitizedFailureReason) {
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(attemptAt, "attemptAt");

        map.compute(clusterId, (id, existingRef) -> {
            if (existingRef == null) {
                // No prior data - create an empty snapshot marking failure but zeros are allowed here
                AdminClientMetricsSnapshot failed = new AdminClientMetricsSnapshot(
                        clusterId,
                        attemptAt,
                        0,
                        0,
                        0,
                        0,
                        0,
                        null,
                        CollectionStatus.FAILURE,
                        null,
                        sanitizedFailureReason,
                        List.of());
                return new AtomicReference<>(failed);
            } else {
                AdminClientMetricsSnapshot prev = existingRef.get();
                // Preserve numeric metrics and brokerLeaderCounts from previous successful snapshot
                AdminClientMetricsSnapshot failed = new AdminClientMetricsSnapshot(
                        clusterId,
                        attemptAt,
                        prev.brokerCount(),
                        prev.topicCount(),
                        prev.partitionCount(),
                        prev.offlinePartitionCount(),
                        prev.underReplicatedPartitionCount(),
                        prev.controllerBrokerId(),
                        CollectionStatus.FAILURE,
                        prev.lastSuccessfulCollectionAt(),
                        sanitizedFailureReason,
                        prev.brokerLeaderCounts());
                existingRef.set(failed);
                return existingRef;
            }
        });
    }

    @Override
    public void remove(UUID clusterId) {
        map.remove(clusterId);
    }

    @Override
    public boolean exists(UUID clusterId) {
        return map.containsKey(clusterId);
    }

    private AdminClientMetricsSnapshot emptySnapshot(UUID clusterId) {
        return new AdminClientMetricsSnapshot(
                clusterId, Instant.EPOCH, 0, 0, 0, 0, 0, null, CollectionStatus.UNKNOWN, null, null, List.of());
    }
}
