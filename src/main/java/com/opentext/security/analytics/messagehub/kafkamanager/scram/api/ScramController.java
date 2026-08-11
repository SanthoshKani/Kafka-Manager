package com.opentext.security.analytics.messagehub.kafkamanager.scram.api;

import com.opentext.security.analytics.messagehub.kafkamanager.scram.service.ScramService;
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
@RequestMapping("/api/v1/clusters/{clusterId}/scram/users")
@Tag(name = "SCRAM", description = "Manage SCRAM credentials (users)")
@SecurityRequirement(name = "bearerAuth")
public class ScramController {

    private final ScramService service;

    public ScramController(ScramService service) {
        this.service = service;
    }

    @GetMapping
    @Operation(
            summary = "Describe SCRAM users",
            description = "Describe SCRAM credentials for the specified user names.",
            tags = {"SCRAM"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "SCRAM users response",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ScramUsersResponse.class)))
    })
    public ScramUsersResponse describe(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId,
            @Parameter(description = "List of usernames to describe") @RequestParam List<String> userNames) {
        return new ScramUsersResponse(service.describe(clusterId, userNames));
    }

    @PutMapping("/{userName}")
    @Operation(
            summary = "Upsert SCRAM credential",
            description = "Create or update SCRAM credential for a user.",
            tags = {"SCRAM"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Credential upserted"),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ScramCredentialUpsertRequest.class)))
    })
    public ResponseEntity<Void> upsert(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId,
            @Parameter(description = "User name", required = true) @PathVariable String userName,
            @Valid @org.springframework.web.bind.annotation.RequestBody ScramCredentialUpsertRequest request) {
        service.upsert(clusterId, userName, request);
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/{userName}")
    @Operation(
            summary = "Delete SCRAM credential",
            description = "Delete SCRAM credential for a user.",
            tags = {"SCRAM"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Credential deleted"),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = ScramCredentialDeleteRequest.class)))
    })
    public ResponseEntity<Void> delete(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId,
            @Parameter(description = "User name", required = true) @PathVariable String userName,
            @Valid @org.springframework.web.bind.annotation.RequestBody ScramCredentialDeleteRequest request) {
        service.delete(clusterId, userName, request);
        return ResponseEntity.noContent().build();
    }
}
