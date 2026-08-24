package com.opentext.security.analytics.messagehub.kafkamanager.brokers.api;

import com.opentext.security.analytics.messagehub.kafkamanager.brokers.service.BrokerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/brokers")
@Tag(name = "Brokers", description = "Broker management and broker-level configs")
@SecurityRequirement(name = "bearerAuth")
public class BrokerController {

    private final BrokerService brokerService;
    private final java.util.UUID defaultClusterId;

    public BrokerController(BrokerService brokerService, java.util.UUID defaultClusterId) {
        this.brokerService = brokerService;
        this.defaultClusterId = defaultClusterId;
    }

    @GetMapping
    @Operation(
            summary = "List brokers",
            description = "List brokers in the cluster.",
            tags = {"Brokers"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "List of brokers",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = BrokerSummaryResponse.class)))
    })
    public List<BrokerSummaryResponse> list() {
        return brokerService.list(defaultClusterId);
    }

    @GetMapping("/{brokerId}/configs")
    @Operation(
            summary = "Broker configs",
            description = "Get broker configuration as key/value pairs.",
            tags = {"Brokers"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Broker configs",
                content = @Content(mediaType = "application/json")),
        @ApiResponse(responseCode = "404", description = "Broker not found")
    })
    public Map<String, String> describeConfigs(
            @Parameter(description = "Broker id", required = true) @PathVariable int brokerId) {
        return brokerService.describeConfigs(defaultClusterId, brokerId);
    }

    @PatchMapping("/{brokerId}/configs")
    @Operation(
            summary = "Alter broker configs",
            description = "Alter broker configuration.",
            tags = {"Brokers"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Configs altered"),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = BrokerConfigMutationRequest.class)))
    })
    public ResponseEntity<Void> alterConfigs(
            @Parameter(description = "Broker id", required = true) @PathVariable int brokerId,
            @Valid @org.springframework.web.bind.annotation.RequestBody BrokerConfigMutationRequest request) {
        brokerService.alterConfigs(defaultClusterId, brokerId, request);
        return ResponseEntity.noContent().build();
    }
}
