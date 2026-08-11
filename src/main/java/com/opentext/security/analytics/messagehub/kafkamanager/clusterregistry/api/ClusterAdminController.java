package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service.ClusterAdminService;
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
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/clusters/{clusterId}/actions")
@Tag(
        name = "Cluster Admin",
        description = "Cluster administrative actions such as leader election, partition reassignment and log dirs")
@SecurityRequirement(name = "bearerAuth")
public class ClusterAdminController {

    private final ClusterAdminService service;

    public ClusterAdminController(ClusterAdminService service) {
        this.service = service;
    }

    @PostMapping("/leader-election")
    @Operation(
            summary = "Preferred leader election",
            description = "Trigger preferred leader election for partitions matching the request.",
            tags = {"Cluster Admin"})
    @ApiResponses({@ApiResponse(responseCode = "204", description = "Leader election scheduled")})
    public ResponseEntity<Void> electLeaders(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId,
            @Valid @org.springframework.web.bind.annotation.RequestBody LeaderElectionRequest request) {
        service.electLeaders(clusterId, request);
        return ResponseEntity.noContent().build();
    }

    @PutMapping("/partition-reassignments")
    @Operation(
            summary = "Start partition reassignment",
            description = "Start a partition reassignment plan.",
            tags = {"Cluster Admin"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Reassignment started"),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = PartitionReassignmentRequest.class)))
    })
    public ResponseEntity<Void> alterPartitionReassignments(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId,
            @Valid @org.springframework.web.bind.annotation.RequestBody PartitionReassignmentRequest request) {
        service.alterPartitionReassignments(clusterId, request);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/partition-reassignments")
    @Operation(
            summary = "List partition reassignments",
            description = "List current partition reassignment operations.",
            tags = {"Cluster Admin"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "List of partition reassignments",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = PartitionReassignmentResponse.class)))
    })
    public List<PartitionReassignmentResponse> listPartitionReassignments(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId) {
        return service.listPartitionReassignments(clusterId);
    }

    @GetMapping("/log-dirs")
    @Operation(
            summary = "Describe log dirs",
            description = "Describe log directories for specified brokers.",
            tags = {"Cluster Admin"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Broker log dir report",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = BrokerLogDirResponse.class)))
    })
    public List<BrokerLogDirResponse> describeLogDirs(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId,
            @Parameter(description = "List of broker ids to query") @RequestParam List<Integer> brokerIds) {
        return service.describeLogDirs(clusterId, brokerIds);
    }

    @PutMapping("/log-dirs")
    @Operation(
            summary = "Alter replica log dirs",
            description = "Move replicas between log directories.",
            tags = {"Cluster Admin"})
    @ApiResponses({@ApiResponse(responseCode = "204", description = "Replica log dirs altered")})
    public ResponseEntity<Void> alterReplicaLogDirs(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId,
            @Valid @org.springframework.web.bind.annotation.RequestBody ReplicaLogDirRequest request) {
        service.alterReplicaLogDirs(clusterId, request);
        return ResponseEntity.noContent().build();
    }
}
