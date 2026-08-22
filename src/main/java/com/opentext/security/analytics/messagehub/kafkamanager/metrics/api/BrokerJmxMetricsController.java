package com.opentext.security.analytics.messagehub.kafkamanager.metrics.api;

import com.opentext.security.analytics.messagehub.kafkamanager.metrics.service.BrokerJmxMetricsCollectorService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exposes the latest broker-side JMX metrics snapshot.
 */
@RestController
@RequestMapping("/api/v1/metrics/broker-jmx")
@Tag(name = "Broker JMX Metrics", description = "Broker-side JMX metrics collected from Kafka JVMs")
@SecurityRequirement(name = "bearerAuth")
public class BrokerJmxMetricsController {

    private final BrokerJmxMetricsCollectorService collectorService;

    public BrokerJmxMetricsController(BrokerJmxMetricsCollectorService collectorService) {
        this.collectorService = collectorService;
    }

    @GetMapping
    @Operation(
            summary = "Get latest broker JMX metrics",
            description =
                    "Return the latest broker-side JMX metrics snapshot with request rates, request latency, topic throughput and idle percentages.")
    @ApiResponses({@ApiResponse(responseCode = "200", description = "Current broker JMX metrics snapshot")})
    public BrokerJmxMetricsCollectorService.BrokerJmxSnapshot current() {
        return collectorService.current();
    }
}
