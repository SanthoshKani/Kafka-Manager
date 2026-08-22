package com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class AdminClientMetricsSnapshotTest {

    @Test
    void constructsImmutableSnapshotAndSanitizesFailureReason() {
        List<BrokerLeaderCount> brokerLeaderCounts =
                new ArrayList<>(List.of(new BrokerLeaderCount(1, 12), new BrokerLeaderCount(2, 8)));

        AdminClientMetricsSnapshot snapshot = new AdminClientMetricsSnapshot(
                UUID.fromString("11111111-1111-1111-1111-111111111111"),
                Instant.parse("2026-08-22T15:00:00Z"),
                2,
                5,
                17,
                1,
                3,
                2,
                CollectionStatus.FAILURE,
                Instant.parse("2026-08-22T14:59:00Z"),
                "  broker metadata timeout  ",
                brokerLeaderCounts);

        brokerLeaderCounts.add(new BrokerLeaderCount(3, 1));

        assertThat(snapshot.clusterId()).isEqualTo(UUID.fromString("11111111-1111-1111-1111-111111111111"));
        assertThat(snapshot.collectedAt()).isEqualTo(Instant.parse("2026-08-22T15:00:00Z"));
        assertThat(snapshot.brokerCount()).isEqualTo(2);
        assertThat(snapshot.topicCount()).isEqualTo(5);
        assertThat(snapshot.partitionCount()).isEqualTo(17);
        assertThat(snapshot.offlinePartitionCount()).isEqualTo(1);
        assertThat(snapshot.underReplicatedPartitionCount()).isEqualTo(3);
        assertThat(snapshot.controllerBrokerId()).isEqualTo(2);
        assertThat(snapshot.collectionStatus()).isEqualTo(CollectionStatus.FAILURE);
        assertThat(snapshot.lastSuccessfulCollectionAt()).isEqualTo(Instant.parse("2026-08-22T14:59:00Z"));
        assertThat(snapshot.sanitizedFailureReason()).isEqualTo("broker metadata timeout");
        assertThat(snapshot.brokerLeaderCounts())
                .containsExactly(new BrokerLeaderCount(1, 12), new BrokerLeaderCount(2, 8));
    }

    @Test
    void rejectsMismatchedBrokerCountAndBrokerLeaderList() {
        assertThatThrownBy(() -> new AdminClientMetricsSnapshot(
                        UUID.randomUUID(),
                        Instant.parse("2026-08-22T15:00:00Z"),
                        2,
                        1,
                        5,
                        0,
                        0,
                        null,
                        CollectionStatus.SUCCESS,
                        Instant.parse("2026-08-22T14:59:00Z"),
                        null,
                        List.of(new BrokerLeaderCount(1, 5))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("brokerCount must match brokerLeaderCounts size");
    }

    @Test
    void rejectsNegativeBrokerLeaderCounts() {
        assertThatThrownBy(() -> new BrokerLeaderCount(-1, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("brokerId must be greater than or equal to zero");
    }

    @Test
    void rejectsNegativeSnapshotCounts() {
        assertThatThrownBy(() -> new AdminClientMetricsSnapshot(
                        UUID.randomUUID(),
                        Instant.parse("2026-08-22T15:00:00Z"),
                        1,
                        -1,
                        5,
                        0,
                        0,
                        null,
                        CollectionStatus.SUCCESS,
                        Instant.parse("2026-08-22T14:59:00Z"),
                        null,
                        List.of(new BrokerLeaderCount(1, 5))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topicCount must be greater than or equal to zero");
    }

    @Test
    void rejectsLastSuccessfulCollectionAfterCollectedAt() {
        assertThatThrownBy(() -> new AdminClientMetricsSnapshot(
                        UUID.randomUUID(),
                        Instant.parse("2026-08-22T15:00:00Z"),
                        1,
                        1,
                        5,
                        0,
                        0,
                        null,
                        CollectionStatus.SUCCESS,
                        Instant.parse("2026-08-22T15:01:00Z"),
                        null,
                        List.of(new BrokerLeaderCount(1, 5))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lastSuccessfulCollectionAt cannot be after collectedAt");
    }
}
