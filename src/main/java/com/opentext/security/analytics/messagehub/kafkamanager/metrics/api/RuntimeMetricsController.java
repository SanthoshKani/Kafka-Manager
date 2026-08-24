package com.opentext.security.analytics.messagehub.kafkamanager.metrics.api;

import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiErrorCode;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiException;
import com.opentext.security.analytics.messagehub.kafkamanager.config.PrometheusScrapeProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminDerivedMetricsStore;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus.MetricMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@ConditionalOnProperty(prefix = "app.features", name = "metrics.enabled", havingValue = "true", matchIfMissing = false)
@RequestMapping("/api/v1")
@Tag(name = "Runtime Metrics", description = "Aggregated runtime metrics from sample store (read-only)")
@SecurityRequirement(name = "bearerAuth")
public class RuntimeMetricsController {

    private final RuntimeMetricSampleStore sampleStore;
    private final AdminDerivedMetricsStore adminStore;
    private final MetricMapper mapper;
    private final PrometheusScrapeProperties properties;
    private final RateCalculator rateCalculator;
    private final java.util.UUID defaultClusterId;

    public RuntimeMetricsController(
            RuntimeMetricSampleStore sampleStore,
            AdminDerivedMetricsStore adminStore,
            MetricMapper mapper,
            PrometheusScrapeProperties properties,
            java.util.UUID defaultClusterId) {
        this.sampleStore = sampleStore;
        this.adminStore = adminStore;
        this.mapper = mapper;
        this.properties = properties;
        this.rateCalculator = new RateCalculator();
        this.defaultClusterId = defaultClusterId;
    }

    private Duration parseWindow(String window) {
        if (window == null || window.isBlank()) return Duration.ofMinutes(1);
        if ("1m".equals(window)) return Duration.ofMinutes(1);
        throw new ApiException(HttpStatus.BAD_REQUEST, ApiErrorCode.VALIDATION_ERROR, "Unsupported window: " + window);
    }

    @GetMapping("/brokers/{brokerId}/metrics")
    @Operation(summary = "Get broker runtime metrics (aggregated)")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Broker metrics"),
        @ApiResponse(responseCode = "400", description = "Invalid window"),
        @ApiResponse(responseCode = "404", description = "Cluster not found")
    })
    public BrokerMetricsResponse brokerMetrics(
            @PathVariable int brokerId, @RequestParam(name = "window", required = false) String window) {

        Duration w = parseWindow(window);
        if (!adminStore.exists(defaultClusterId))
            throw new ApiException(HttpStatus.NOT_FOUND, ApiErrorCode.NOT_FOUND, "Cluster not found");

        Instant now = Instant.now();
        Instant windowStart = now.minus(w);

        AdminClientMetricsSnapshot admin = adminStore.getCurrent(defaultClusterId);

        // list of canonical metrics to report at broker scope
        List<String> counters = List.of(
                "bytes.in.total",
                "bytes.out.total",
                "messages.in.total",
                "failed.produce.requests.total",
                "failed.fetch.requests.total",
                "bytes.rejected.total",
                "leader.elections.total",
                "leader.elections.unclean.total",
                "isr.expands.total",
                "isr.shrinks.total");
        List<String> gauges = List.of(
                "network.processor.avg_idle",
                "request.handler.avg_idle",
                "broker.leader.count",
                "broker.under_replicated.partitions",
                "controller.active.count",
                "controller.offline.partitions");

        List<MetricValue> values = new ArrayList<>();

        // process counters -> derive per-minute rates
        for (String canonical : counters) {
            MetricIdentity id = new MetricIdentity(defaultClusterId, brokerId, null, canonical);
            MetricSample latest = sampleStore.latest(id);
            MetricSample earliest = sampleStore.earliestAtOrBefore(id, windowStart);
            MetricValue mv = new MetricValue(canonical, null, null, null, false, "UNAVAILABLE", null);
            if (latest != null && earliest != null) {
                var res = rateCalculator.calculate(earliest, latest);
                if (res instanceof RateCalculator.RateResult.ValidRate vr) {
                    mv = new MetricValue(
                            canonical,
                            Double.valueOf(vr.ratePerMinute()),
                            latest.timestamp(),
                            latest.sourceBackend().name(),
                            true,
                            "OK",
                            latest.unit());
                } else if (res instanceof RateCalculator.RateResult.CounterReset) {
                    mv = new MetricValue(
                            canonical,
                            null,
                            latest.timestamp(),
                            latest.sourceBackend().name(),
                            false,
                            "COUNTER_RESET",
                            latest.unit());
                } else if (res instanceof RateCalculator.RateResult.InsufficientData) {
                    mv = new MetricValue(
                            canonical,
                            null,
                            latest.timestamp(),
                            latest.sourceBackend().name(),
                            false,
                            "INSUFFICIENT_DATA",
                            latest.unit());
                } else if (res instanceof RateCalculator.RateResult.InvalidSample is) {
                    mv = new MetricValue(
                            canonical,
                            null,
                            latest.timestamp(),
                            latest.sourceBackend().name(),
                            false,
                            "INVALID_SAMPLE",
                            latest.unit());
                }
            } else if (latest != null) {
                mv = new MetricValue(
                        canonical,
                        null,
                        latest.timestamp(),
                        latest.sourceBackend().name(),
                        false,
                        "INSUFFICIENT_DATA",
                        latest.unit());
            }
            values.add(mv);
        }

        // process gauges -> RollingGaugeAggregator
        RollingGaugeAggregator agg = new RollingGaugeAggregator(w);
        for (String canonical : gauges) {
            MetricIdentity id = new MetricIdentity(defaultClusterId, brokerId, null, canonical);
            List<MetricSample> samples = sampleStore.samplesInRange(id, windowStart, now);
            MetricValue mv = new MetricValue(canonical, null, null, null, false, "UNAVAILABLE", null);
            var ar = agg.aggregate(samples, now);
            if (ar instanceof RollingGaugeAggregator.AggregationResult.Valid v) {
                Instant lastSampleAt = samples.isEmpty()
                        ? null
                        : samples.get(samples.size() - 1).timestamp();
                String backend = samples.isEmpty()
                        ? null
                        : samples.get(samples.size() - 1).sourceBackend().name();
                mv = new MetricValue(
                        canonical, Double.valueOf(v.average()), lastSampleAt, backend, true, "OK", "ratio");
            } else if (ar instanceof RollingGaugeAggregator.AggregationResult.InsufficientData) {
                // find latest sample if any
                MetricSample latest = sampleStore.latest(id);
                mv = new MetricValue(
                        canonical,
                        null,
                        latest == null ? null : latest.timestamp(),
                        latest == null ? null : latest.sourceBackend().name(),
                        false,
                        "INSUFFICIENT_DATA",
                        "ratio");
            } else if (ar instanceof RollingGaugeAggregator.AggregationResult.InvalidSample inv) {
                MetricSample latest = sampleStore.latest(id);
                mv = new MetricValue(
                        canonical,
                        null,
                        latest == null ? null : latest.timestamp(),
                        latest == null ? null : latest.sourceBackend().name(),
                        false,
                        "INVALID_SAMPLE",
                        "ratio");
            }
            values.add(mv);
        }

        Instant runtimeCollectedAt = values.stream()
                .map(MetricValue::getLastSampleAt)
                .filter(Objects::nonNull)
                .max(Comparator.naturalOrder())
                .orElse(null);

        return new BrokerMetricsResponse(
                defaultClusterId, brokerId, w.toString(), runtimeCollectedAt, admin.collectedAt(), values);
    }

    @GetMapping("/topics/{topic}/metrics")
    @Operation(summary = "Get topic aggregated metrics across brokers (cluster-topic)")
    public TopicMetricsResponse topicMetrics(
            @PathVariable String topic,
            @RequestParam(name = "window", required = false) String window,
            @RequestParam(name = "perBroker", required = false) boolean perBroker) {
        Duration w = parseWindow(window);
        if (!adminStore.exists(defaultClusterId))
            throw new ApiException(HttpStatus.NOT_FOUND, ApiErrorCode.NOT_FOUND, "Cluster not found");
        Instant now = Instant.now();
        Instant windowStart = now.minus(w);

        // canonical topic counters
        List<String> topicCounters = List.of(
                "bytes.in.total",
                "bytes.out.total",
                "messages.in.total",
                "failed.produce.requests.total",
                "failed.fetch.requests.total",
                "bytes.rejected.total");

        // determine brokers from adminStore
        AdminClientMetricsSnapshot admin = adminStore.getCurrent(defaultClusterId);
        List<Integer> brokers =
                admin.brokerLeaderCounts().stream().map(b -> b.brokerId()).collect(Collectors.toList());

        Map<String, AggregatedMetric> aggregates = new HashMap<>();
        Map<Integer, List<MetricValue>> perBrokerMap = new HashMap<>();

        for (String canonical : topicCounters) {
            double sum = 0.0;
            boolean anyAvailable = false;
            boolean someInsufficient = false;
            for (int brokerId : brokers) {
                MetricIdentity id = new MetricIdentity(defaultClusterId, brokerId, topic, canonical);
                MetricSample latest = sampleStore.latest(id);
                MetricSample earliest = sampleStore.earliestAtOrBefore(id, windowStart);
                MetricValue mv;
                if (latest != null && earliest != null) {
                    var res = rateCalculator.calculate(earliest, latest);
                    if (res instanceof RateCalculator.RateResult.ValidRate vr) {
                        mv = new MetricValue(
                                canonical,
                                Double.valueOf(vr.ratePerMinute()),
                                latest.timestamp(),
                                latest.sourceBackend().name(),
                                true,
                                "OK",
                                latest.unit());
                        sum += vr.ratePerMinute();
                        anyAvailable = true;
                    } else if (res instanceof RateCalculator.RateResult.CounterReset) {
                        mv = new MetricValue(
                                canonical,
                                null,
                                latest.timestamp(),
                                latest.sourceBackend().name(),
                                false,
                                "COUNTER_RESET",
                                latest.unit());
                        someInsufficient = true;
                    } else {
                        mv = new MetricValue(
                                canonical,
                                null,
                                latest == null ? null : latest.timestamp(),
                                latest == null ? null : latest.sourceBackend().name(),
                                false,
                                "INSUFFICIENT_DATA",
                                latest == null ? null : latest.unit());
                        someInsufficient = true;
                    }
                } else if (latest != null) {
                    mv = new MetricValue(
                            canonical,
                            null,
                            latest.timestamp(),
                            latest.sourceBackend().name(),
                            false,
                            "INSUFFICIENT_DATA",
                            latest.unit());
                    someInsufficient = true;
                } else {
                    mv = new MetricValue(canonical, null, null, null, false, "UNAVAILABLE", null);
                }
                perBrokerMap.computeIfAbsent(brokerId, k -> new ArrayList<>()).add(mv);
            }
            aggregates.put(
                    canonical,
                    new AggregatedMetric(
                            canonical, anyAvailable ? Double.valueOf(sum) : null, anyAvailable, someInsufficient));
        }

        return new TopicMetricsResponse(
                defaultClusterId,
                topic,
                w.toString(),
                admin.collectedAt(),
                aggregates,
                perBroker ? perBrokerMap : null);
    }

    @GetMapping("/metrics")
    @Operation(summary = "Get cluster aggregated metrics")
    public ClusterMetricsResponse clusterMetrics(@RequestParam(name = "window", required = false) String window) {
        Duration w = parseWindow(window);
        if (!adminStore.exists(defaultClusterId))
            throw new ApiException(HttpStatus.NOT_FOUND, ApiErrorCode.NOT_FOUND, "Cluster not found");
        Instant now = Instant.now();
        Instant windowStart = now.minus(w);
        AdminClientMetricsSnapshot admin = adminStore.getCurrent(defaultClusterId);

        // expected brokers
        int expected = admin.brokerCount();
        List<Integer> brokers =
                admin.brokerLeaderCounts().stream().map(b -> b.brokerId()).collect(Collectors.toList());
        int fresh = 0;
        int stale = 0;
        int unavailable = 0;

        // evaluate per-broker freshness using a simple metric: broker.leader.count latest sample
        for (int brokerId : brokers) {
            MetricIdentity id = new MetricIdentity(defaultClusterId, brokerId, null, "broker.leader.count");
            MetricSample latest = sampleStore.latest(id);
            if (latest == null) {
                unavailable++;
                continue;
            }
            Instant last = latest.timestamp();
            if (last.isAfter(windowStart)) fresh++;
            else stale++;
        }

        // compose overall summary
        CollectionHealth health = new CollectionHealth(expected, fresh, stale, unavailable);

        return new ClusterMetricsResponse(defaultClusterId, admin.collectedAt(), Instant.now(), health, null);
    }

    // DTOs
    public static record MetricValue(
            String name,
            Double value,
            Instant lastSampleAt,
            String sourceBackend,
            boolean available,
            String status,
            String unit) {
        public Double getValue() {
            return value;
        }

        public Instant getLastSampleAt() {
            return lastSampleAt;
        }
    }

    public static record BrokerMetricsResponse(
            UUID clusterId,
            int brokerId,
            String window,
            Instant runtimeCollectedAt,
            Instant adminCollectedAt,
            List<MetricValue> metrics) {}

    public static record AggregatedMetric(
            String name, Double aggregatedValue, boolean anyAvailable, boolean someInsufficient) {}

    public static record TopicMetricsResponse(
            UUID clusterId,
            String topic,
            String window,
            Instant adminCollectedAt,
            Map<String, AggregatedMetric> aggregates,
            Map<Integer, List<MetricValue>> perBroker) {}

    public static record CollectionHealth(int expected, int fresh, int stale, int unavailable) {}

    public static record ClusterMetricsResponse(
            UUID clusterId, Instant adminCollectedAt, Instant diagnosticsAt, CollectionHealth health, Object details) {}
}
