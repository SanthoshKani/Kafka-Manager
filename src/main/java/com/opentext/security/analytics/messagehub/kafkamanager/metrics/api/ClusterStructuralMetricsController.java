package com.opentext.security.analytics.messagehub.kafkamanager.metrics.api;

import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiErrorCode;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ApiException;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ResourceNotFoundException;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminClientMetricsSnapshot;
import com.opentext.security.analytics.messagehub.kafkamanager.metrics.domain.AdminDerivedMetricsStore;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * Cluster-scoped structural metrics endpoint. Returns cached data only and never invokes AdminClient.
 */
@RestController
@ConditionalOnProperty(prefix = "app.features", name = "metrics.enabled", havingValue = "true", matchIfMissing = false)
@RequestMapping("/api/v1/metrics/structural")
@Tag(name = "Structural Metrics", description = "Cluster-health metrics derived from Kafka AdminClient metadata")
@SecurityRequirement(name = "bearerAuth")
public class ClusterStructuralMetricsController {

    private final AdminDerivedMetricsStore store;
    private final java.util.UUID defaultClusterId;

    public ClusterStructuralMetricsController(AdminDerivedMetricsStore store, java.util.UUID defaultClusterId) {
        this.store = store;
        this.defaultClusterId = defaultClusterId;
    }

    @GetMapping
    @Operation(
            summary = "Get structural cluster metrics (cached)",
            description =
                    "Return the latest cached Admin-derived structural metrics for the cluster. Does not query Kafka.")
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Current structural metrics snapshot (may be last-known-good)"),
        @ApiResponse(responseCode = "404", description = "Cluster not found"),
        @ApiResponse(responseCode = "503", description = "Metrics not yet available")
    })
    public AdminClientMetricsSnapshot current() {
        // If the store has no entry for this cluster we treat it as unknown
        if (!store.exists(defaultClusterId)) {
            throw new ResourceNotFoundException("Cluster not found");
        }

        AdminClientMetricsSnapshot snapshot = store.getCurrent(defaultClusterId);
        // If we have never had a successful collection, return 503 Service Unavailable
        if (snapshot.lastSuccessfulCollectionAt() == null) {
            throw new ApiException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    ApiErrorCode.DEPENDENCY_FAILURE,
                    "Structural metrics are not yet available for cluster");
        }
        return snapshot;
    }
}
