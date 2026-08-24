package com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api;

import com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.service.ConsumerGroupService;
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
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/consumer-groups")
@Tag(name = "Consumer Groups", description = "Manage consumer groups: list, describe, delete, offsets")
@SecurityRequirement(name = "bearerAuth")
public class ConsumerGroupController {

    private final ConsumerGroupService service;
    private final java.util.UUID defaultClusterId;

    public ConsumerGroupController(ConsumerGroupService service, java.util.UUID defaultClusterId) {
        this.service = service;
        this.defaultClusterId = defaultClusterId;
    }

    @GetMapping
    @Operation(
            summary = "List consumer groups",
            description = "List consumer groups in the cluster.",
            tags = {"Consumer Groups"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "List of consumer groups",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ConsumerGroupSummaryResponse.class)))
    })
    public List<ConsumerGroupSummaryResponse> list() {
        return service.list(defaultClusterId);
    }

    @GetMapping("/{groupId}")
    @Operation(
            summary = "Describe consumer group",
            description = "Get details for a consumer group.",
            tags = {"Consumer Groups"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Consumer group detail",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ConsumerGroupDetailResponse.class))),
        @ApiResponse(responseCode = "404", description = "Consumer group not found")
    })
    public ConsumerGroupDetailResponse describe(
            @Parameter(description = "Consumer group id", required = true) @PathVariable String groupId) {
        return service.describe(defaultClusterId, groupId);
    }

    @DeleteMapping("/{groupId}")
    @Operation(
            summary = "Delete consumer group",
            description = "Delete a consumer group.",
            tags = {"Consumer Groups"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Consumer group deleted"),
        @ApiResponse(responseCode = "404", description = "Consumer group not found")
    })
    public ResponseEntity<Void> delete(
            @Parameter(description = "Consumer group id", required = true) @PathVariable String groupId) {
        service.delete(defaultClusterId, groupId);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{groupId}/offsets")
    @Operation(
            summary = "Alter consumer group offsets",
            description = "Alter offsets for a consumer group.",
            tags = {"Consumer Groups"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Offsets altered"),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ConsumerGroupOffsetUpdateRequest.class)))
    })
    public ResponseEntity<Void> alterOffsets(
            @Parameter(description = "Consumer group id", required = true) @PathVariable String groupId,
            @Valid @org.springframework.web.bind.annotation.RequestBody ConsumerGroupOffsetUpdateRequest request) {
        service.alterOffsets(defaultClusterId, groupId, request);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{groupId}/members/remove")
    @Operation(
            summary = "Remove members from consumer group",
            description = "Remove specified members from the consumer group.",
            tags = {"Consumer Groups"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Members removed"),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ConsumerGroupMemberRemovalRequest.class)))
    })
    public ResponseEntity<Void> removeMembers(
            @Parameter(description = "Consumer group id", required = true) @PathVariable String groupId,
            @Valid @org.springframework.web.bind.annotation.RequestBody ConsumerGroupMemberRemovalRequest request) {
        service.removeMembers(defaultClusterId, groupId, request);
        return ResponseEntity.noContent().build();
    }
}
