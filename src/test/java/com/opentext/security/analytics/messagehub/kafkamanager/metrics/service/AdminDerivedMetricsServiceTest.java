package com.opentext.security.analytics.messagehub.kafkamanager.metrics.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

class AdminDerivedMetricsServiceTest {

    @Test
    void deriveSnapshotCalculatesStructuralMetrics() {
        AdminDerivedMetricsService service = new AdminDerivedMetricsService(
                null,
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry(),
                new com.opentext.security.analytics.messagehub.kafkamanager.metrics.service
                        .InMemoryAdminDerivedMetricsStore());

        Map<String, TopicDescription> descriptions = Map.of(
                "alpha", topic("alpha", List.of(partition(0, 0, 2, 2), partition(1, -1, 2, 1))),
                "beta", topic("beta", List.of(partition(0, 1, 1, 1))));

        var snapshot = service.deriveSnapshot(descriptions, 1, Instant.parse("2026-08-22T00:00:00Z"));

        assertThat(snapshot.partitionCount()).isEqualTo(3);
        assertThat(snapshot.offlinePartitions()).isEqualTo(1);
        assertThat(snapshot.underReplicatedPartitions()).isEqualTo(1);
        assertThat(snapshot.activeControllerCount()).isEqualTo(1);
        assertThat(snapshot.leaderCounts()).containsEntry(0, 1).containsEntry(1, 1);
    }

    private TopicDescription topic(String name, List<TopicPartitionInfo> infos) {
        return new TopicDescription(name, false, infos);
    }

    private TopicPartitionInfo partition(int index, int leaderId, int replicaCount, int isrCount) {
        Node leader = leaderId < 0 ? null : new Node(leaderId, "broker" + leaderId, 9092);
        List<Node> replicas = java.util.stream.IntStream.range(0, replicaCount)
                .mapToObj(i -> new Node(i, "broker" + i, 9092))
                .toList();
        List<Node> isr = java.util.stream.IntStream.range(0, isrCount)
                .mapToObj(i -> new Node(i, "broker" + i, 9092))
                .toList();
        return new TopicPartitionInfo(index, leader, replicas, isr);
    }
}
