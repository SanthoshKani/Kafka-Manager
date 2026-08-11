package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service.ClusterRegistryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/clusters")
@Tag(name = "Clusters", description = "Cluster registry and cluster-level operations")
@SecurityRequirement(name = "bearerAuth")
public class ClusterController {

    private final ClusterRegistryService service;

    public ClusterController(ClusterRegistryService service) {
        this.service = service;
    }

    @GetMapping
    @Operation(
            summary = "List clusters",
            description = "List registered clusters with pagination.",
            tags = {"Clusters"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Paged cluster list",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ClusterSummaryResponse.class)))
    })
    public Page<ClusterSummaryResponse> list(
            @Parameter(description = "Page number (zero-based)") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size") @RequestParam(defaultValue = "20") int size) {
        return service.list(page, size);
    }

    @GetMapping("/{clusterId}")
    @Operation(
            summary = "Get cluster",
            description = "Get details for a registered cluster.",
            tags = {"Clusters"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Cluster detail",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ClusterDetailResponse.class))),
        @ApiResponse(responseCode = "404", description = "Cluster not found")
    })
    public ClusterDetailResponse get(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId) {
        return service.get(clusterId);
    }

    @PostMapping
    @Operation(
            summary = "Register cluster",
            description = "Register a new cluster or validate the request in dry-run mode.",
            tags = {"Clusters"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Validation result (dry-run)",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ClusterValidationResponse.class))),
        @ApiResponse(
                responseCode = "201",
                description = "Cluster registered",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ClusterDetailResponse.class))),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request",
                content = @Content(mediaType = "application/json"))
    })
    public ResponseEntity<?> register(
            @Valid @org.springframework.web.bind.annotation.RequestBody RegisterClusterRequest request) {
        if (request.dryRun()) {
            return ResponseEntity.ok(service.validateUnsaved(request));
        }
        ClusterDetailResponse response = service.register(request);
        return ResponseEntity.created(URI.create("/api/v1/clusters/" + response.id()))
                .body(response);
    }

    @PatchMapping("/{clusterId}")
    @Operation(
            summary = "Update cluster",
            description = "Update cluster registration details.",
            tags = {"Clusters"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Updated cluster detail",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ClusterDetailResponse.class))),
        @ApiResponse(responseCode = "404", description = "Cluster not found")
    })
    public ClusterDetailResponse update(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId,
            @org.springframework.web.bind.annotation.RequestBody UpdateClusterRequest request) {
        return service.update(clusterId, request);
    }

    @PostMapping("/{clusterId}/actions/validate")
    @Operation(
            summary = "Validate cluster connectivity",
            description = "Validate an already-registered cluster's connectivity and capabilities.",
            tags = {"Clusters"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Validation result",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ClusterValidationResponse.class)))
    })
    public ClusterValidationResponse validate(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId) {
        return service.validateSavedCluster(clusterId);
    }

    @PostMapping("/actions/validate")
    @Operation(
            summary = "Validate cluster (unsaved)",
            description = "Validate cluster configuration without registering it.",
            tags = {"Clusters"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Validation result",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ClusterValidationResponse.class)))
    })
    public ClusterValidationResponse validateUnsaved(
            @Valid @org.springframework.web.bind.annotation.RequestBody RegisterClusterRequest request) {
        return service.validateUnsaved(request);
    }

    @PostMapping("/{clusterId}/actions/enable")
    @Operation(
            summary = "Enable cluster",
            description = "Enable a registered cluster.",
            tags = {"Clusters"})
    @ApiResponses({@ApiResponse(responseCode = "204", description = "Cluster enabled")})
    public ResponseEntity<Void> enable(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId) {
        service.enable(clusterId);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{clusterId}/actions/disable")
    @Operation(
            summary = "Disable cluster",
            description = "Disable a registered cluster.",
            tags = {"Clusters"})
    @ApiResponses({@ApiResponse(responseCode = "204", description = "Cluster disabled")})
    public ResponseEntity<Void> disable(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId) {
        service.disable(clusterId);
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/{clusterId}")
    @Operation(
            summary = "Delete cluster",
            description = "Delete a registered cluster.",
            tags = {"Clusters"})
    @ApiResponses({@ApiResponse(responseCode = "204", description = "Cluster deleted")})
    public ResponseEntity<Void> delete(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId) {
        service.delete(clusterId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{clusterId}/capabilities")
    @Operation(
            summary = "Cluster capabilities",
            description = "Report cluster capabilities and feature availability.",
            tags = {"Clusters"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Capability report",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = CapabilityReportResponse.class)))
    })
    public CapabilityReportResponse capabilities(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId) {
        return service.capabilityReport(clusterId);
    }
}
