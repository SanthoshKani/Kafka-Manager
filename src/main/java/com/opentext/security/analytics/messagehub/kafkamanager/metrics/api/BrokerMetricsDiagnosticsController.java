package com.opentext.security.analytics.messagehub.kafkamanager.metrics.api;

import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiErrorCode;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiException;
import com.opentext.security.analytics.messagehub.kafkamanager.config.PrometheusScrapeProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminDerivedMetricsStore;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus.MetricMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricIdentity;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.MetricSample;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.runtime.RuntimeMetricSampleStore;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Administrator-only diagnostics for recognized runtime metric names for a single broker.
 */
@RestController
@ConditionalOnProperty(prefix = "app.features", name = "metrics.enabled", havingValue = "true", matchIfMissing = false)
@RequestMapping("/api/v1/brokers/{brokerId}/metrics/diagnostics")
@Tag(name = "Metrics Diagnostics", description = "Diagnostic information about recognized runtime metric names")
@SecurityRequirement(name = "bearerAuth")
public class BrokerMetricsDiagnosticsController {

    private final MetricMapper mapper;
    private final AdminDerivedMetricsStore adminStore;
    private final PrometheusScrapeProperties properties;
    private final RuntimeMetricSampleStore sampleStore;
    private final java.util.UUID defaultClusterId;

    public BrokerMetricsDiagnosticsController(
            MetricMapper mapper,
            AdminDerivedMetricsStore adminStore,
            PrometheusScrapeProperties properties,
            RuntimeMetricSampleStore sampleStore,
            java.util.UUID defaultClusterId) {
        this.mapper = mapper;
        this.adminStore = adminStore;
        this.properties = properties;
        this.sampleStore = sampleStore;
        this.defaultClusterId = defaultClusterId;
    }

    @GetMapping
    @Operation(summary = "List recognized metric names and label keys for a broker")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "List of recognized metrics"),
        @ApiResponse(responseCode = "403", description = "Forbidden or diagnostics disabled"),
        @ApiResponse(responseCode = "404", description = "Cluster not found")
    })
    public DiagnosticsResponse diagnostics(
            @Parameter(description = "Broker id", required = true) @PathVariable int brokerId) {

        // Feature gating: diagnostics disabled by default in production
        if (!properties.diagnosticsEnabled()) {
            throw new ApiException(HttpStatus.FORBIDDEN, ApiErrorCode.INVALID_STATE, "Diagnostics disabled");
        }

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (!isAdmin(auth)) {
            throw new ApiException(
                    HttpStatus.FORBIDDEN, ApiErrorCode.KAFKA_AUTHORIZATION_FAILURE, "Administrator role required");
        }

        // Ensure cluster exists in admin-derived store
        if (!adminStore.exists(defaultClusterId)) {
            throw new ApiException(HttpStatus.NOT_FOUND, ApiErrorCode.NOT_FOUND, "Cluster not found");
        }

        // Collect recognized metric names and label keys, enforcing a response size limit
        int max = Math.max(1, properties.diagnosticsMaxItems());
        List<MetricInfo> metrics = mapper.mapKeys().stream()
                .limit(max)
                .map(s -> {
                    var desc = mapper.get(s);
                    var labels = desc == null
                            ? List.<String>of()
                            : desc.labelKeys == null
                                    ? List.<String>of()
                                    : desc.labelKeys.stream().collect(Collectors.toList());
                    return new MetricInfo(s, labels);
                })
                .collect(Collectors.toList());

        AdminClientMetricsSnapshot snapshot = adminStore.getCurrent(defaultClusterId);

        // Now build statuses for a small set of important canonical metrics (controller, leader elections, ISR, log
        // flush)
        java.util.Set<String> interesting = java.util.Set.of(
                "controller.active.count",
                "controller.offline.partitions",
                "leader.elections.total",
                "leader.elections.unclean.total",
                "broker.leader.count",
                "broker.under_replicated.partitions",
                "isr.expands.total",
                "isr.shrinks.total",
                "log.flush.total",
                "log.flush.one_minute_rate");

        List<MetricStatus> statuses = interesting.stream()
                .map(canonical -> {
                    // find source names that map to this canonical
                    List<String> sources = mapper.mapKeys().stream()
                            .filter(k -> {
                                var d = mapper.get(k);
                                return d != null && canonical.equals(d.canonicalName);
                            })
                            .collect(Collectors.toList());

                    // check runtime store for latest sample for this canonical name at the broker scope
                    MetricIdentity id = new MetricIdentity(defaultClusterId, brokerId, null, canonical);
                    MetricSample latest = sampleStore.latest(id);
                    Instant lastSampleAt = latest == null ? null : latest.timestamp();
                    boolean fresh = lastSampleAt != null
                            && lastSampleAt.isAfter(Instant.now().minus(properties.sampleRetention()));

                    // admin-derived comparison values where applicable
                    Long adminValue = null;
                    // controller active: compare controllerBrokerId presence
                    if ("controller.active.count".equals(canonical)) {
                        adminValue = snapshot.controllerBrokerId() == null ? 0L : 1L;
                    }
                    // broker leader count: find in brokerLeaderCounts
                    if ("broker.leader.count".equals(canonical)) {
                        var bl = snapshot.brokerLeaderCounts().stream()
                                .filter(b -> b.brokerId() == brokerId)
                                .findFirst();
                        if (bl.isPresent()) adminValue = (long) bl.get().leaderPartitionCount();
                    }

                    boolean mismatch = false;
                    if (adminValue != null && latest != null) {
                        // try to compare numeric latest value to adminValue when latest is gauge or derived counter
                        try {
                            double v = latest.value();
                            mismatch = Math.abs(v - adminValue) > 0.001;
                        } catch (Exception ignored) {
                        }
                    }

                    return new MetricStatus(canonical, sources, lastSampleAt, fresh, adminValue, mismatch);
                })
                .collect(Collectors.toList());

        return new DiagnosticsResponse(defaultClusterId, brokerId, Instant.now(), metrics, snapshot, statuses);
    }

    private boolean isAdmin(Authentication auth) {
        if (auth == null || !auth.isAuthenticated()) return false;
        for (GrantedAuthority ga : auth.getAuthorities()) {
            String a = ga.getAuthority();
            if ("ROLE_ADMIN".equals(a) || "ADMIN".equals(a)) return true;
        }
        return false;
    }

    public static record MetricInfo(String sourceName, List<String> labelKeys) {}

    public static record MetricStatus(
            String canonicalName,
            List<String> sourceNames,
            Instant lastSampleAt,
            boolean fresh,
            Long adminValue,
            boolean mismatch) {}

    public static record DiagnosticsResponse(
            UUID clusterId,
            int brokerId,
            Instant diagnosticsAt,
            List<MetricInfo> recognizedMetrics,
            AdminClientMetricsSnapshot adminSnapshot,
            List<MetricStatus> metricStatuses) {}
}
