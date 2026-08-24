package com.opentext.security.analytics.messagehub.kafkamanager.metrics.service;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.BrokerLeaderCount;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.CollectionStatus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Computes and exposes Admin-derived Kafka structural metrics.
 *
 * <p>The service polls Kafka metadata on a schedule, derives cluster health metrics, and publishes
 * the latest snapshot to Micrometer gauges and the REST controller layer.
 */
@Service
@ConditionalOnProperty(prefix = "app.features", name = "metrics.enabled", havingValue = "true", matchIfMissing = false)
public class AdminDerivedMetricsService {

    private static final Logger log = LoggerFactory.getLogger(AdminDerivedMetricsService.class);

    private static final UUID METRICS_CLUSTER_ID =
            UUID.nameUUIDFromBytes("kafka-manager-admin-derived-metrics".getBytes(StandardCharsets.UTF_8));

    private final AdminDerivedMetricsCollector collector;
    private final MeterRegistry meterRegistry;
    private final com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminDerivedMetricsStore store;
    private final ConcurrentHashMap<java.util.UUID, ClusterMeters> clusterMeters;
    private final ConcurrentHashMap<java.util.UUID, Object> inProgress;

    public AdminDerivedMetricsService(
            AdminDerivedMetricsCollector collector,
            MeterRegistry meterRegistry,
            com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminDerivedMetricsStore store) {
        this.collector = collector;
        this.meterRegistry = meterRegistry;
        this.store = store;
        this.clusterMeters = new ConcurrentHashMap<>();
        this.inProgress = new ConcurrentHashMap<>();
    }

    /**
     * Remove any meters associated with the provided cluster id. This does not remove stored snapshots.
     * Call when a cluster is deleted to avoid stale meters.
     */
    public void removeClusterMeters(UUID clusterId) {
        ClusterMeters meters = clusterMeters.remove(clusterId);
        if (meters == null) {
            return;
        }
        // Remove known cluster-level meters
        String[] clusterMetricNames = new String[] {
            "kafka.manager.cluster.broker.count",
            "kafka.manager.cluster.topic.count",
            "kafka.manager.cluster.partition.count",
            "kafka.manager.cluster.offline.partition.count",
            "kafka.manager.cluster.under.replicated.partition.count",
            "kafka.manager.cluster.controller.present",
            "kafka.manager.cluster.metrics.age.seconds",
            "kafka.manager.cluster.metrics.collection.healthy"
        };
        for (String name : clusterMetricNames) {
            try {
                var metersToRemove = meterRegistry
                        .get(name)
                        .tag("clusterId", clusterId.toString())
                        .meters();
                for (var m : metersToRemove) {
                    meterRegistry.remove(m);
                }
            } catch (Exception ignored) {
            }
        }
        // Remove broker-level leader gauges
        try {
            var brokers = meters.brokerLeaderCounts.keySet();
            for (Integer brokerId : brokers) {
                try {
                    var ms = meterRegistry
                            .get("kafka.manager.broker.leader.partition.count")
                            .tags("clusterId", clusterId.toString(), "brokerId", String.valueOf(brokerId))
                            .meters();
                    for (var m : ms) {
                        meterRegistry.remove(m);
                    }
                } catch (Exception ignored) {
                }
            }
        } catch (Exception ignored) {
        }
    }

    /**
     * Ensure meters exist for the given cluster and return the holder.
     */
    private ClusterMeters ensureClusterMeters(UUID clusterId) {
        return clusterMeters.computeIfAbsent(clusterId, id -> {
            ClusterMeters meters = new ClusterMeters(id);
            // Register cluster-scoped gauges with clusterId tag
            Gauge.builder("kafka.manager.cluster.broker.count", meters.brokerCount, AtomicInteger::get)
                    .tag("clusterId", id.toString())
                    .description("Number of brokers in the cluster")
                    .baseUnit("count")
                    .register(meterRegistry);
            Gauge.builder("kafka.manager.cluster.topic.count", meters.topicCount, AtomicInteger::get)
                    .tag("clusterId", id.toString())
                    .description("Number of topics in the cluster")
                    .baseUnit("count")
                    .register(meterRegistry);
            Gauge.builder("kafka.manager.cluster.partition.count", meters.partitionCount, AtomicLong::get)
                    .tag("clusterId", id.toString())
                    .description("Total partitions observed in the cluster")
                    .baseUnit("count")
                    .register(meterRegistry);
            Gauge.builder("kafka.manager.cluster.offline.partition.count", meters.offlinePartitions, AtomicLong::get)
                    .tag("clusterId", id.toString())
                    .description("Count of offline partitions derived from topic metadata")
                    .baseUnit("count")
                    .register(meterRegistry);
            Gauge.builder(
                            "kafka.manager.cluster.under.replicated.partition.count",
                            meters.underReplicatedPartitions,
                            AtomicLong::get)
                    .tag("clusterId", id.toString())
                    .description("Count of under-replicated partitions derived from topic metadata")
                    .baseUnit("count")
                    .register(meterRegistry);
            Gauge.builder("kafka.manager.cluster.controller.present", meters.controllerPresent, AtomicInteger::get)
                    .tag("clusterId", id.toString())
                    .description("Controller presence (1 when controller known, 0 otherwise)")
                    .baseUnit("state")
                    .register(meterRegistry);
            Gauge.builder("kafka.manager.cluster.metrics.age.seconds", meters.metricsAgeSeconds, AtomicLong::get)
                    .tag("clusterId", id.toString())
                    .description("Seconds since last successful metrics collection")
                    .baseUnit("seconds")
                    .register(meterRegistry);
            Gauge.builder("kafka.manager.cluster.metrics.collection.healthy", meters.metricsHealthy, AtomicInteger::get)
                    .tag("clusterId", id.toString())
                    .description("Whether the last collection completed successfully (1=yes,0=no)")
                    .baseUnit("state")
                    .register(meterRegistry);
            return meters;
        });
    }

    /**
     * Apply a snapshot to the registered meters for the cluster. Registers meters lazily.
     */
    public void refreshMetersForCluster(UUID clusterId) {
        AdminClientMetricsSnapshot snapshot = store.getCurrent(clusterId);
        ClusterMeters meters = ensureClusterMeters(clusterId);
        meters.brokerCount.set(snapshot.brokerCount());
        meters.topicCount.set(snapshot.topicCount());
        meters.partitionCount.set(snapshot.partitionCount());
        meters.offlinePartitions.set(snapshot.offlinePartitionCount());
        meters.underReplicatedPartitions.set(snapshot.underReplicatedPartitionCount());
        meters.controllerPresent.set(snapshot.controllerBrokerId() == null ? 0 : 1);
        meters.metricsHealthy.set(
                snapshot.collectionStatus()
                                == com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain
                                        .CollectionStatus.SUCCESS
                        ? 1
                        : 0);
        // metricsAgeSeconds = seconds since lastSuccessfulCollectionAt if present, otherwise since collectedAt
        Instant ref = snapshot.lastSuccessfulCollectionAt() == null
                ? snapshot.collectedAt()
                : snapshot.lastSuccessfulCollectionAt();
        long age = ref == null || ref.equals(Instant.EPOCH)
                ? Long.MAX_VALUE
                : java.time.Duration.between(ref, Instant.now()).getSeconds();
        meters.metricsAgeSeconds.set(age < 0 ? 0 : age);
        updateLeaderCountGauges(clusterId, snapshot.brokerLeaderCounts());
    }

    /**
     * Refresh the latest snapshot on a fixed schedule.
     */
    @Scheduled(fixedDelayString = "${app.metrics.admin-derived.poll-interval:PT30S}")
    public void refresh() {
        // Poll the single configured METRICS_CLUSTER_ID. The collector checks app.metrics.admin-derived.enabled
        // and will return an empty snapshot if disabled.
        java.util.UUID clusterId = METRICS_CLUSTER_ID;
        // Prevent overlapping collection for same cluster
        Object marker = new Object();
        Object prev = inProgress.putIfAbsent(clusterId, marker);
        if (prev != null) {
            // Already collecting for this cluster
            return;
        }

        try {
            try {
                AdminClientMetricsSnapshot snapshot = computeSnapshot();
                // Save to store; store will set lastSuccessfulCollectionAt on SUCCESS
                store.saveSuccessful(snapshot);
                // Refresh meters for this cluster from store snapshot
                refreshMetersForCluster(clusterId);
            } catch (RuntimeException exception) {
                // Record failure without wiping previous successful values
                String sanitized = exception.getMessage() == null
                        ? exception.getClass().getSimpleName()
                        : exception.getMessage().trim();
                if (sanitized.length() > 256) {
                    sanitized = sanitized.substring(0, 256);
                }
                store.recordFailure(clusterId, java.time.Instant.now(), sanitized);
                // Refresh meters to reflect failure state while preserving last-known-good values
                refreshMetersForCluster(clusterId);
                // Log concise warning without credentials
                log.warn("Admin-derived metrics collection failed for cluster {}: {}", clusterId, sanitized);
            }
        } finally {
            inProgress.remove(clusterId);
        }
    }

    /**
     * Warm up the snapshot as soon as the application is ready.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void warmUp() {
        refresh();
    }

    /**
     * Return the latest snapshot captured by the background refresh.
     *
     * @return latest structural metrics snapshot
     */
    public AdminClientMetricsSnapshot current() {
        return store.getCurrent(METRICS_CLUSTER_ID);
    }

    /**
     * Compute the snapshot immediately using the Kafka AdminClient.
     *
     * @return fresh structural metrics snapshot
     */
    public AdminClientMetricsSnapshot computeSnapshot() {
        return collector == null ? emptySnapshot(METRICS_CLUSTER_ID) : collector.collect(METRICS_CLUSTER_ID);
    }

    private AdminClientMetricsSnapshot emptySnapshot(UUID clusterId) {
        return new AdminClientMetricsSnapshot(
                clusterId, Instant.EPOCH, 0, 0, 0, 0, 0, null, CollectionStatus.UNKNOWN, null, null, List.of());
    }

    ClusterStructuralMetrics deriveSnapshot(
            Map<String, TopicDescription> descriptions, int activeControllerCount, Instant capturedAt) {
        long offlinePartitions = 0;
        long underReplicatedPartitions = 0;
        int partitionCount = 0;
        Map<Integer, Integer> leaderCounts = new LinkedHashMap<>();

        for (TopicDescription description : descriptions.values()) {
            partitionCount += description.partitions().size();
            for (var partition : description.partitions()) {
                if (partition.leader() == null || partition.leader().id() < 0) {
                    offlinePartitions++;
                }
                if (partition.isr().size() < partition.replicas().size()) {
                    underReplicatedPartitions++;
                }
                if (partition.leader() != null && partition.leader().id() >= 0) {
                    leaderCounts.merge(partition.leader().id(), 1, Integer::sum);
                }
            }
        }

        return new ClusterStructuralMetrics(
                capturedAt,
                offlinePartitions,
                underReplicatedPartitions,
                partitionCount,
                activeControllerCount,
                Map.copyOf(leaderCounts));
    }

    private void updateLeaderCountGauges(List<BrokerLeaderCount> leaderCounts) {
        // Deprecated single-cluster leader count update; kept for compatibility but no-op
    }

    private void updateLeaderCountGauges(UUID clusterId, List<BrokerLeaderCount> leaderCounts) {
        ClusterMeters meters = ensureClusterMeters(clusterId);
        Map<Integer, Integer> leaderCountMap = new LinkedHashMap<>();
        for (BrokerLeaderCount leaderCount : leaderCounts) {
            leaderCountMap.put(leaderCount.brokerId(), leaderCount.leaderPartitionCount());
        }
        // Ensure gauges exist for brokers and set values
        leaderCountMap.forEach((brokerId, count) -> meters.brokerLeaderCounts
                .computeIfAbsent(brokerId, id -> registerBrokerLeaderGauge(clusterId, id))
                .set(count));
        // For any existing broker gauges not present in new map, set to 0
        meters.brokerLeaderCounts.forEach((brokerId, gauge) -> gauge.set(leaderCountMap.getOrDefault(brokerId, 0)));
    }

    private AtomicInteger registerLeaderCountGauge(Integer brokerId) {
        // Legacy single-cluster support removed in favor of per-cluster broker gauges
        return new AtomicInteger();
    }

    private AtomicInteger registerBrokerLeaderGauge(UUID clusterId, Integer brokerId) {
        AtomicInteger gaugeValue = new AtomicInteger();
        Gauge.builder("kafka.manager.broker.leader.partition.count", gaugeValue, AtomicInteger::get)
                .tag("clusterId", clusterId.toString())
                .tag("brokerId", String.valueOf(brokerId))
                .description("Count of partitions currently led by the broker")
                .baseUnit("count")
                .register(meterRegistry);
        return gaugeValue;
    }

    /**
     * Holder for per-cluster meter backing objects.
     */
    private static final class ClusterMeters {
        final UUID clusterId;
        final AtomicInteger brokerCount = new AtomicInteger();
        final AtomicInteger topicCount = new AtomicInteger();
        final AtomicLong partitionCount = new AtomicLong();
        final AtomicLong offlinePartitions = new AtomicLong();
        final AtomicLong underReplicatedPartitions = new AtomicLong();
        final AtomicInteger controllerPresent = new AtomicInteger();
        final AtomicLong metricsAgeSeconds = new AtomicLong();
        final AtomicInteger metricsHealthy = new AtomicInteger();
        final ConcurrentHashMap<Integer, AtomicInteger> brokerLeaderCounts = new ConcurrentHashMap<>();

        ClusterMeters(UUID clusterId) {
            this.clusterId = clusterId;
        }
    }

    /**
     * Snapshot of computed structural metrics.
     *
     * @param capturedAt snapshot capture time
     * @param offlinePartitions count of partitions without a leader
     * @param underReplicatedPartitions count of partitions whose ISR is smaller than the replica set
     * @param partitionCount total partitions across topics
     * @param activeControllerCount controller presence indicator (0 or 1)
     * @param leaderCounts count of partitions led by broker id
     */
    public record ClusterStructuralMetrics(
            Instant capturedAt,
            long offlinePartitions,
            long underReplicatedPartitions,
            int partitionCount,
            int activeControllerCount,
            Map<Integer, Integer> leaderCounts) {}
}
