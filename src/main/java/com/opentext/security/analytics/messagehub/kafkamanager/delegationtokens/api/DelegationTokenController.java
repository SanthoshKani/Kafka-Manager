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
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/delegation-tokens")
@Tag(name = "Delegation Tokens", description = "Manage delegation tokens: list, create, renew, expire")
@SecurityRequirement(name = "bearerAuth")
public class DelegationTokenController {

    private final DelegationTokenService service;
    private final java.util.UUID defaultClusterId;

    public DelegationTokenController(DelegationTokenService service, java.util.UUID defaultClusterId) {
        this.service = service;
        this.defaultClusterId = defaultClusterId;
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
    public List<DelegationTokenResponse> list() {
        return service.list(defaultClusterId);
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
            @Valid @org.springframework.web.bind.annotation.RequestBody DelegationTokenCreateRequest request) {
        return service.create(defaultClusterId, request);
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
            @Parameter(description = "Token id", required = true) @PathVariable String tokenId,
            @Valid @org.springframework.web.bind.annotation.RequestBody DelegationTokenRenewRequest request) {
        return service.renew(defaultClusterId, tokenId, request);
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
            @Parameter(description = "Token id", required = true) @PathVariable String tokenId,
            @Valid @org.springframework.web.bind.annotation.RequestBody DelegationTokenExpireRequest request) {
        service.expire(defaultClusterId, tokenId, request);
        return ResponseEntity.noContent().build();
    }
}
