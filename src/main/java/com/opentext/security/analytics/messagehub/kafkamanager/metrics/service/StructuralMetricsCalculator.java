package com.opentext.security.analytics.messagehub.kafkamanager.metrics.service;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.BrokerLeaderCount;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.jspecify.annotations.Nullable;

/**
 * Pure Kafka structural-metrics calculator.
 *
 * <p>This class is intentionally free of Spring, Micrometer, AdminClient, and network access so it
 * can be tested with pre-fetched Kafka metadata.
 */
public final class StructuralMetricsCalculator {

    public AdminClientMetricsSnapshot calculate(
            UUID clusterId,
            Collection<Node> brokerMetadata,
            Collection<TopicDescription> topicDescriptions,
            @Nullable Node controllerMetadata,
            Instant collectedAt) {
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(brokerMetadata, "brokerMetadata");
        Objects.requireNonNull(topicDescriptions, "topicDescriptions");
        Objects.requireNonNull(collectedAt, "collectedAt");

        Map<Integer, Integer> leaderCounts = new LinkedHashMap<>();
        brokerMetadata.stream().map(Node::id).distinct().sorted().forEach(brokerId -> leaderCounts.put(brokerId, 0));

        long offlinePartitions = 0;
        long underReplicatedPartitions = 0;
        int topicCount = topicDescriptions.size();
        int partitionCount = 0;

        for (TopicDescription topicDescription : topicDescriptions) {
            for (TopicPartitionInfo partition : topicDescription.partitions()) {
                partitionCount++;
                Node leader = partition.leader();
                if (leader == null || leader.id() < 0) {
                    offlinePartitions++;
                }
                if (partition.isr().size() < partition.replicas().size()) {
                    underReplicatedPartitions++;
                }
                if (leader != null && leader.id() >= 0 && leaderCounts.containsKey(leader.id())) {
                    leaderCounts.merge(leader.id(), 1, Integer::sum);
                }
            }
        }

        Integer controllerBrokerId =
                controllerMetadata == null || controllerMetadata.id() < 0 ? null : controllerMetadata.id();
        List<BrokerLeaderCount> brokerLeaderCounts = leaderCounts.entrySet().stream()
                .map(entry -> new BrokerLeaderCount(entry.getKey(), entry.getValue()))
                .toList();

        return new AdminClientMetricsSnapshot(
                clusterId,
                collectedAt,
                brokerLeaderCounts.size(),
                topicCount,
                partitionCount,
                offlinePartitions,
                underReplicatedPartitions,
                controllerBrokerId,
                CollectionStatus.SUCCESS,
                collectedAt,
                null,
                brokerLeaderCounts);
    }
}
