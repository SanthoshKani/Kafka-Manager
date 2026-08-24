package com.opentext.security.analytics.messagehub.kafkamanager.metrics.api;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.service.AdminDerivedMetricsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * Exposes the latest Admin-derived structural Kafka metrics.
 */
@RestController
@ConditionalOnProperty(prefix = "app.features", name = "metrics.enabled", havingValue = "true", matchIfMissing = false)
// This controller exposes the same conceptual endpoint as ClusterStructuralMetricsController.
// Keep a distinct path to avoid mapping collisions. Prefer ClusterStructuralMetricsController for
// the canonical '/api/v1/metrics/structural' endpoint.
@RequestMapping("/api/v1/metrics/structural/service")
@Tag(name = "Structural Metrics", description = "Cluster-health metrics derived from Kafka AdminClient metadata")
@SecurityRequirement(name = "bearerAuth")
public class StructuralMetricsController {

    private final AdminDerivedMetricsService metricsService;

    public StructuralMetricsController(AdminDerivedMetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @GetMapping
    @Operation(
            summary = "Get structural cluster metrics",
            description =
                    "Return the latest snapshot of broker count, topic count, partition count, offline partitions, under-replicated partitions, controller broker id and leader counts.")
    @ApiResponses({@ApiResponse(responseCode = "200", description = "Current structural metrics snapshot")})
    public AdminClientMetricsSnapshot current() {
        return metricsService.current();
    }
}
