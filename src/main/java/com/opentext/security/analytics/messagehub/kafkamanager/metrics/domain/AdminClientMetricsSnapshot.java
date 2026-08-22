package com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

public record AdminClientMetricsSnapshot(
        UUID clusterId,
        Instant collectedAt,
        int brokerCount,
        int topicCount,
        int partitionCount,
        long offlinePartitionCount,
        long underReplicatedPartitionCount,
        @Nullable Integer controllerBrokerId,
        CollectionStatus collectionStatus,
        @Nullable Instant lastSuccessfulCollectionAt,
        @Nullable String sanitizedFailureReason,
        List<BrokerLeaderCount> brokerLeaderCounts) {

    public AdminClientMetricsSnapshot {
        clusterId = Objects.requireNonNull(clusterId, "clusterId");
        collectedAt = Objects.requireNonNull(collectedAt, "collectedAt");
        collectionStatus = Objects.requireNonNull(collectionStatus, "collectionStatus");
        brokerLeaderCounts = List.copyOf(Objects.requireNonNull(brokerLeaderCounts, "brokerLeaderCounts"));

        validateNonNegative("brokerCount", brokerCount);
        validateNonNegative("topicCount", topicCount);
        validateNonNegative("partitionCount", partitionCount);
        validateNonNegative("offlinePartitionCount", offlinePartitionCount);
        validateNonNegative("underReplicatedPartitionCount", underReplicatedPartitionCount);

        if (controllerBrokerId != null && controllerBrokerId < 0) {
            throw new IllegalArgumentException("controllerBrokerId must be greater than or equal to zero");
        }
        if (brokerCount != brokerLeaderCounts.size()) {
            throw new IllegalArgumentException("brokerCount must match brokerLeaderCounts size");
        }
        if (lastSuccessfulCollectionAt != null && lastSuccessfulCollectionAt.isAfter(collectedAt)) {
            throw new IllegalArgumentException("lastSuccessfulCollectionAt cannot be after collectedAt");
        }

        sanitizedFailureReason = sanitizeFailureReason(sanitizedFailureReason);
    }

    private static void validateNonNegative(String fieldName, long value) {
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " must be greater than or equal to zero");
        }
    }

    private static @Nullable String sanitizeFailureReason(@Nullable String failureReason) {
        if (failureReason == null) {
            return null;
        }
        String sanitized = failureReason.trim();
        return sanitized.isEmpty() ? null : sanitized;
    }
}
