package com.opentext.security.analytics.messagehub.kafkamanager.metrics.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.BrokerLeaderCount;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AdminDerivedMetricsServiceMetersTest {

    private SimpleMeterRegistry meterRegistry;
    private InMemoryAdminDerivedMetricsStore store;
    private AdminDerivedMetricsService service;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        store = new InMemoryAdminDerivedMetricsStore();
        service = new AdminDerivedMetricsService(null, meterRegistry, store);
    }

    @Test
    void registersAndUpdatesClusterGauges() {
        UUID clusterId = UUID.randomUUID();
        Instant now = Instant.now();
        AdminClientMetricsSnapshot snapshot = new AdminClientMetricsSnapshot(
                clusterId,
                now,
                2,
                3,
                6,
                1,
                0,
                1,
                com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus.SUCCESS,
                now,
                null,
                List.of(new BrokerLeaderCount(1, 4), new BrokerLeaderCount(2, 2)));

        store.saveSuccessful(snapshot);
        service.refreshMetersForCluster(clusterId);

        double partitions = meterRegistry
                .get("kafka.manager.cluster.partition.count")
                .tag("clusterId", clusterId.toString())
                .gauge()
                .value();
        assertThat(partitions).isEqualTo(6.0);

        double offline = meterRegistry
                .get("kafka.manager.cluster.offline.partition.count")
                .tag("clusterId", clusterId.toString())
                .gauge()
                .value();
        assertThat(offline).isEqualTo(1.0);

        double broker1 = meterRegistry
                .get("kafka.manager.broker.leader.partition.count")
                .tag("clusterId", clusterId.toString())
                .tag("brokerId", "1")
                .gauge()
                .value();
        assertThat(broker1).isEqualTo(4.0);
    }
}
