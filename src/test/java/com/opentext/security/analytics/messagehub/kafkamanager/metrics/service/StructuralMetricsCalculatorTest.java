package com.opentext.security.analytics.messagehub.kafkamanager.metrics.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.BrokerLeaderCount;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

class StructuralMetricsCalculatorTest {

    private static final UUID CLUSTER_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final Instant COLLECTED_AT = Instant.parse("2026-08-22T00:00:00Z");

    private final StructuralMetricsCalculator calculator = new StructuralMetricsCalculator();

    @Test
    void calculatesHealthyReplicatedPartitions() {
        AdminClientMetricsSnapshot snapshot = calculate(
                List.of(node(1), node(2)),
                List.of(topic("orders", false, List.of(partition(0, 1, 2, 2), partition(1, 2, 2, 2)))),
                node(1));

        assertThat(snapshot.clusterId()).isEqualTo(CLUSTER_ID);
        assertThat(snapshot.brokerCount()).isEqualTo(2);
        assertThat(snapshot.topicCount()).isEqualTo(1);
        assertThat(snapshot.partitionCount()).isEqualTo(2);
        assertThat(snapshot.offlinePartitionCount()).isZero();
        assertThat(snapshot.underReplicatedPartitionCount()).isZero();
        assertThat(snapshot.controllerBrokerId()).isEqualTo(1);
        assertThat(snapshot.collectionStatus()).isEqualTo(CollectionStatus.SUCCESS);
        assertThat(snapshot.lastSuccessfulCollectionAt()).isEqualTo(COLLECTED_AT);
        assertThat(snapshot.sanitizedFailureReason()).isNull();
        assertThat(snapshot.brokerLeaderCounts())
                .containsExactly(new BrokerLeaderCount(1, 1), new BrokerLeaderCount(2, 1));
    }

    @Test
    void calculatesOfflinePartitionWhenLeaderIsMissing() {
        AdminClientMetricsSnapshot snapshot =
                calculate(List.of(node(1)), List.of(topic("orders", false, List.of(partition(null, 2, 2)))), null);

        assertThat(snapshot.offlinePartitionCount()).isEqualTo(1);
        assertThat(snapshot.underReplicatedPartitionCount()).isZero();
        assertThat(snapshot.controllerBrokerId()).isNull();
        assertThat(snapshot.brokerLeaderCounts()).containsExactly(new BrokerLeaderCount(1, 0));
    }

    @Test
    void calculatesUnderReplicatedPartitionWhenIsrIsSmallerThanReplicaSet() {
        AdminClientMetricsSnapshot snapshot =
                calculate(List.of(node(1)), List.of(topic("orders", false, List.of(partition(1, 2, 1)))), node(1));

        assertThat(snapshot.offlinePartitionCount()).isZero();
        assertThat(snapshot.underReplicatedPartitionCount()).isEqualTo(1);
        assertThat(snapshot.brokerLeaderCounts()).containsExactly(new BrokerLeaderCount(1, 1));
    }

    @Test
    void calculatesOfflineAndUnderReplicatedAtTheSameTime() {
        AdminClientMetricsSnapshot snapshot = calculate(
                List.of(node(1), node(2)), List.of(topic("orders", false, List.of(partition(null, 2, 1)))), node(2));

        assertThat(snapshot.offlinePartitionCount()).isEqualTo(1);
        assertThat(snapshot.underReplicatedPartitionCount()).isEqualTo(1);
        assertThat(snapshot.brokerLeaderCounts())
                .containsExactly(new BrokerLeaderCount(1, 0), new BrokerLeaderCount(2, 0));
    }

    @Test
    void calculatesMultipleTopicsAndBrokers() {
        AdminClientMetricsSnapshot snapshot = calculate(
                List.of(node(1), node(2), node(3)),
                List.of(
                        topic("orders", false, List.of(partition(0, 1, 2, 2), partition(1, 2, 3, 2))),
                        topic("payments", false, List.of(partition(0, 1, 1, 1)))),
                node(2));

        assertThat(snapshot.topicCount()).isEqualTo(2);
        assertThat(snapshot.partitionCount()).isEqualTo(3);
        assertThat(snapshot.offlinePartitionCount()).isZero();
        assertThat(snapshot.underReplicatedPartitionCount()).isEqualTo(1);
        assertThat(snapshot.brokerLeaderCounts())
                .containsExactly(new BrokerLeaderCount(1, 2), new BrokerLeaderCount(2, 1), new BrokerLeaderCount(3, 0));
    }

    @Test
    void includesBrokerWithZeroLeaders() {
        AdminClientMetricsSnapshot snapshot = calculate(
                List.of(node(1), node(2)), List.of(topic("orders", false, List.of(partition(0, 1, 2, 2)))), node(1));

        assertThat(snapshot.brokerLeaderCounts())
                .containsExactly(new BrokerLeaderCount(1, 1), new BrokerLeaderCount(2, 0));
    }

    @Test
    void handlesEmptyClusterMetadata() {
        AdminClientMetricsSnapshot snapshot = calculate(List.of(), List.of(), null);

        assertThat(snapshot.brokerCount()).isZero();
        assertThat(snapshot.topicCount()).isZero();
        assertThat(snapshot.partitionCount()).isZero();
        assertThat(snapshot.offlinePartitionCount()).isZero();
        assertThat(snapshot.underReplicatedPartitionCount()).isZero();
        assertThat(snapshot.controllerBrokerId()).isNull();
        assertThat(snapshot.collectionStatus()).isEqualTo(CollectionStatus.SUCCESS);
        assertThat(snapshot.lastSuccessfulCollectionAt()).isEqualTo(COLLECTED_AT);
        assertThat(snapshot.brokerLeaderCounts()).isEmpty();
    }

    @Test
    void excludesInternalTopicOnlyWhenCallerOmitsIt() {
        TopicDescription internalTopic = topic("__consumer_offsets", true, List.of(partition(0, 1, 1, 1)));
        AdminClientMetricsSnapshot snapshot =
                calculate(List.of(node(1)), List.of(topic("orders", false, List.of(partition(0, 1, 1, 1)))), node(1));

        assertThat(internalTopic.isInternal()).isTrue();
        assertThat(snapshot.topicCount()).isEqualTo(1);
        assertThat(snapshot.partitionCount()).isEqualTo(1);
        assertThat(snapshot.brokerLeaderCounts()).containsExactly(new BrokerLeaderCount(1, 1));
    }

    private AdminClientMetricsSnapshot calculate(List<Node> brokers, List<TopicDescription> topics, Node controller) {
        return calculator.calculate(CLUSTER_ID, brokers, topics, controller, COLLECTED_AT);
    }

    private TopicDescription topic(String name, boolean internal, List<TopicPartitionInfo> partitions) {
        return new TopicDescription(name, internal, partitions);
    }

    private TopicPartitionInfo partition(Integer leaderId, int replicaCount, int isrCount) {
        return partition(0, leaderId, replicaCount, isrCount);
    }

    private TopicPartitionInfo partition(int partition, Integer leaderId, int replicaCount, int isrCount) {
        Node leader = leaderId == null ? null : node(leaderId);
        List<Node> replicas = nodes(replicaCount);
        List<Node> isr = nodes(isrCount);
        return new TopicPartitionInfo(partition, leader, replicas, isr);
    }

    private List<Node> nodes(int count) {
        return IntStream.range(0, count).mapToObj(this::node).toList();
    }

    private Node node(int id) {
        return new Node(id, "broker" + id, 9092);
    }
}
