package com.opentext.security.analytics.messagehub.kafkamanager.metrics.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.BrokerLeaderCount;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class InMemoryAdminDerivedMetricsStoreTest {

    @Test
    void saveSuccessAndFailurePreserveValues() {
        InMemoryAdminDerivedMetricsStore store = new InMemoryAdminDerivedMetricsStore();
        UUID clusterId = UUID.randomUUID();
        Instant collectedAt = Instant.parse("2026-08-22T00:00:00Z");

        AdminClientMetricsSnapshot success = new AdminClientMetricsSnapshot(
                clusterId,
                collectedAt,
                2,
                1,
                3,
                1,
                0,
                1,
                CollectionStatus.SUCCESS,
                collectedAt,
                null,
                List.of(new BrokerLeaderCount(0, 2), new BrokerLeaderCount(1, 1)));

        store.saveSuccessful(success);

        AdminClientMetricsSnapshot stored = store.getCurrent(clusterId);
        assertThat(stored.collectionStatus()).isEqualTo(CollectionStatus.SUCCESS);
        assertThat(stored.lastSuccessfulCollectionAt()).isEqualTo(collectedAt);
        assertThat(stored.brokerCount()).isEqualTo(2);

        // Now record failure - should preserve numeric values
        Instant attempt = Instant.parse("2026-08-22T00:01:00Z");
        store.recordFailure(clusterId, attempt, "Timeout connecting to kafka");

        AdminClientMetricsSnapshot afterFailure = store.getCurrent(clusterId);
        assertThat(afterFailure.collectionStatus()).isEqualTo(CollectionStatus.FAILURE);
        assertThat(afterFailure.collectedAt()).isEqualTo(attempt);
        assertThat(afterFailure.brokerCount()).isEqualTo(2);
        assertThat(afterFailure.lastSuccessfulCollectionAt()).isEqualTo(collectedAt);
        assertThat(afterFailure.sanitizedFailureReason()).contains("Timeout");

        // Remove cluster
        store.remove(clusterId);
        AdminClientMetricsSnapshot cleared = store.getCurrent(clusterId);
        assertThat(cleared.collectionStatus()).isEqualTo(CollectionStatus.UNKNOWN);
    }
}
