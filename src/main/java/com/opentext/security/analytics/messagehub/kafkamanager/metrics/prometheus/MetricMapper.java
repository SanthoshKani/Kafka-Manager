package com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSemanticType;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Maps source Prometheus metric names to canonical internal names and semantic types.
 */
@Component
public final class MetricMapper {

    private final Map<String, Descriptor> map = new HashMap<>();

    public enum SourceUnit {
        RAW, // unit unknown or raw count
        PERCENT, // source reports percentage 0..100
        RATIO // source reports ratio 0..1
    }

    public static final class Descriptor {
        public final String canonicalName;
        public final MetricSemanticType semanticType;
        public final String unit; // normalized unit name to expose, e.g. "count", "per_minute", "ratio"
        public final SourceUnit sourceUnit;
        public final java.util.Set<String> labelKeys;

        public Descriptor(String canonicalName, MetricSemanticType semanticType) {
            this(canonicalName, semanticType, "count", SourceUnit.RAW, java.util.Set.of());
        }

        public Descriptor(String canonicalName, MetricSemanticType semanticType, String unit, SourceUnit sourceUnit) {
            this(canonicalName, semanticType, unit, sourceUnit, java.util.Set.of());
        }

        public Descriptor(
                String canonicalName,
                MetricSemanticType semanticType,
                String unit,
                SourceUnit sourceUnit,
                java.util.Set<String> labelKeys) {
            this.canonicalName = Objects.requireNonNull(canonicalName);
            this.semanticType = Objects.requireNonNull(semanticType);
            this.unit = Objects.requireNonNull(unit);
            this.sourceUnit = Objects.requireNonNull(sourceUnit);
            this.labelKeys = labelKeys == null ? java.util.Set.of() : java.util.Set.copyOf(labelKeys);
        }
    }

    public MetricMapper() {
        // default mappings for broker-topic counters (prometheus exporter _total style)
        map.put(
                "kafka_server_broker_topic_metrics_bytes_in_total",
                new Descriptor(
                        "bytes.in.total",
                        MetricSemanticType.MONOTONIC_COUNTER,
                        "bytes",
                        SourceUnit.RAW,
                        java.util.Set.of("topic")));
        map.put(
                "kafka_server_broker_topic_metrics_bytes_out_total",
                new Descriptor(
                        "bytes.out.total",
                        MetricSemanticType.MONOTONIC_COUNTER,
                        "bytes",
                        SourceUnit.RAW,
                        java.util.Set.of("topic")));
        map.put(
                "kafka_server_broker_topic_metrics_messages_in_total",
                new Descriptor(
                        "messages.in.total",
                        MetricSemanticType.MONOTONIC_COUNTER,
                        "count",
                        SourceUnit.RAW,
                        java.util.Set.of("topic")));

        // failed requests and bytes rejected (prefer monotonic counts when exporter provides them)
        map.put(
                "kafka_server_broker_topic_metrics_failed_produce_requests_total",
                new Descriptor(
                        "failed.produce.requests.total",
                        MetricSemanticType.MONOTONIC_COUNTER,
                        "count",
                        SourceUnit.RAW,
                        java.util.Set.of("topic")));
        map.put(
                "kafka_server_broker_topic_metrics_failed_fetch_requests_total",
                new Descriptor(
                        "failed.fetch.requests.total",
                        MetricSemanticType.MONOTONIC_COUNTER,
                        "count",
                        SourceUnit.RAW,
                        java.util.Set.of("topic")));
        map.put(
                "kafka_server_broker_topic_metrics_bytes_rejected_total",
                new Descriptor(
                        "bytes.rejected.total",
                        MetricSemanticType.MONOTONIC_COUNTER,
                        "bytes",
                        SourceUnit.RAW,
                        java.util.Set.of("topic")));

        // percent/ratio idle metrics from network/request handler (source may report percent 0..100 or ratio 0..1)
        map.put(
                "kafka_network_processor_avg_idle_percent",
                new Descriptor("network.processor.avg_idle", MetricSemanticType.GAUGE, "ratio", SourceUnit.PERCENT));
        map.put(
                "kafka_network_request_handler_avg_idle_percent",
                new Descriptor("request.handler.avg_idle", MetricSemanticType.GAUGE, "ratio", SourceUnit.PERCENT));

        // Controller-side metrics (common exporter names)
        map.put(
                "kafka_controller_active_controller_count",
                new Descriptor("controller.active.count", MetricSemanticType.GAUGE, "count", SourceUnit.RAW));
        map.put(
                "kafka_controller_offline_partitions_count",
                new Descriptor("controller.offline.partitions", MetricSemanticType.GAUGE, "count", SourceUnit.RAW));
        map.put(
                "kafka_controller_leader_election_total",
                new Descriptor(
                        "leader.elections.total", MetricSemanticType.MONOTONIC_COUNTER, "count", SourceUnit.RAW));
        map.put(
                "kafka_controller_unclean_leader_election_total",
                new Descriptor(
                        "leader.elections.unclean.total",
                        MetricSemanticType.MONOTONIC_COUNTER,
                        "count",
                        SourceUnit.RAW));

        // Broker-side leadership/replication metrics
        map.put(
                "kafka_server_replicamanager_leader_count",
                new Descriptor("broker.leader.count", MetricSemanticType.GAUGE, "count", SourceUnit.RAW));
        map.put(
                "kafka_server_replica_manager_under_replicated_partitions",
                new Descriptor(
                        "broker.under_replicated.partitions", MetricSemanticType.GAUGE, "count", SourceUnit.RAW));
        map.put(
                "kafka_server_replicamanager_isr_expands_total",
                new Descriptor("isr.expands.total", MetricSemanticType.MONOTONIC_COUNTER, "count", SourceUnit.RAW));
        map.put(
                "kafka_server_replicamanager_isr_shrinks_total",
                new Descriptor("isr.shrinks.total", MetricSemanticType.MONOTONIC_COUNTER, "count", SourceUnit.RAW));

        // Log flush metrics (names vary by exporter; prefer cumulative counts when available)
        map.put(
                "kafka_log_flush_total",
                new Descriptor("log.flush.total", MetricSemanticType.MONOTONIC_COUNTER, "count", SourceUnit.RAW));
        map.put(
                "kafka_log_flush_one_minute_rate",
                new Descriptor(
                        "log.flush.one_minute_rate", MetricSemanticType.GAUGE, "count_per_minute", SourceUnit.RAW));
    }

    public void register(String source, String canonical, MetricSemanticType type) {
        map.put(source, new Descriptor(canonical, type));
    }

    public Descriptor get(String source) {
        return map.get(source);
    }

    public java.util.Set<String> mapKeys() {
        return java.util.Collections.unmodifiableSet(map.keySet());
    }

    public java.util.Map<String, String> toNameMap() {
        java.util.Map<String, String> out = new java.util.HashMap<>();
        for (var e : map.entrySet()) out.put(e.getKey(), e.getValue().canonicalName);
        return out;
    }
}
