package com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.api;

import com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.service.DelegationTokenService;
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
@RequestMapping("/api/v1/clusters/{clusterId}/delegation-tokens")
@Tag(name = "Delegation Tokens", description = "Manage delegation tokens: list, create, renew, expire")
@SecurityRequirement(name = "bearerAuth")
public class DelegationTokenController {

    private final DelegationTokenService service;

    public DelegationTokenController(DelegationTokenService service) {
        this.service = service;
    }

    @GetMapping
    @Operation(
            summary = "List delegation tokens",
            description = "List delegation tokens in the cluster.",
            tags = {"Delegation Tokens"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "List of tokens",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = DelegationTokenResponse.class)))
    })
    public List<DelegationTokenResponse> list(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId) {
        return service.list(clusterId);
    }

    @PostMapping
    @Operation(
            summary = "Create delegation token",
            description = "Create a new delegation token.",
            tags = {"Delegation Tokens"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Created token",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = DelegationTokenResponse.class))),
        @ApiResponse(responseCode = "400", description = "Invalid request")
    })
    public DelegationTokenResponse create(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId,
            @Valid @org.springframework.web.bind.annotation.RequestBody DelegationTokenCreateRequest request) {
        return service.create(clusterId, request);
    }

    @PostMapping("/{tokenId}/renew")
    @Operation(
            summary = "Renew token",
            description = "Renew an existing delegation token.",
            tags = {"Delegation Tokens"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Renewed token",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = DelegationTokenResponse.class))),
        @ApiResponse(responseCode = "400", description = "Invalid request")
    })
    public DelegationTokenResponse renew(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId,
            @Parameter(description = "Token id", required = true) @PathVariable String tokenId,
            @Valid @org.springframework.web.bind.annotation.RequestBody DelegationTokenRenewRequest request) {
        return service.renew(clusterId, tokenId, request);
    }

    @PostMapping("/{tokenId}/expire")
    @Operation(
            summary = "Expire token",
            description = "Expire a delegation token.",
            tags = {"Delegation Tokens"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Token expired"),
        @ApiResponse(responseCode = "400", description = "Invalid request")
    })
    public ResponseEntity<Void> expire(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId,
            @Parameter(description = "Token id", required = true) @PathVariable String tokenId,
            @Valid @org.springframework.web.bind.annotation.RequestBody DelegationTokenExpireRequest request) {
        service.expire(clusterId, tokenId, request);
        return ResponseEntity.noContent().build();
    }
}
