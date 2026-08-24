package com.opentext.security.analytics.messagehub.kafkamanager.clientmetrics.api;

import com.opentext.security.analytics.messagehub.kafkamanager.clientmetrics.service.ClientMetricsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/client-metrics")
@Tag(name = "Client Metrics", description = "List client-metrics resources exposed by clients")
@SecurityRequirement(name = "bearerAuth")
public class ClientMetricsController {

    private final ClientMetricsService service;
    private final java.util.UUID defaultClusterId;

    public ClientMetricsController(ClientMetricsService service, java.util.UUID defaultClusterId) {
        this.service = service;
        this.defaultClusterId = defaultClusterId;
    }

    @GetMapping
    @Operation(
            summary = "List client metrics resources",
            description = "List client metrics resources discovered for cluster clients.",
            tags = {"Client Metrics"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "List of client metric resources",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ClientMetricResourceResponse.class)))
    })
    public List<ClientMetricResourceResponse> list() {
        return service.list(defaultClusterId);
    }
}
