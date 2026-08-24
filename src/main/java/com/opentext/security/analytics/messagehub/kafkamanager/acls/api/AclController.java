package com.opentext.security.analytics.messagehub.kafkamanager.acls.api;

import com.opentext.security.analytics.messagehub.kafkamanager.acls.service.AclService;
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
@RequestMapping("/api/v1/acls")
@Tag(name = "ACLs", description = "Manage ACLs: list, create, delete")
@SecurityRequirement(name = "bearerAuth")
public class AclController {

    private final AclService service;
    private final java.util.UUID defaultClusterId;

    public AclController(AclService service, java.util.UUID defaultClusterId) {
        this.service = service;
        this.defaultClusterId = defaultClusterId;
    }

    @GetMapping
    @Operation(
            summary = "List ACLs",
            description = "List ACLs with optional filters.",
            tags = {"ACLs"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "List of ACLs",
                content =
                        @Content(mediaType = "application/json", schema = @Schema(implementation = AclResponse.class)))
    })
    public List<AclResponse> list(
            @Parameter(description = "Resource type filter") @RequestParam(required = false) String resourceType,
            @Parameter(description = "Resource name filter") @RequestParam(required = false) String resourceName,
            @Parameter(description = "Pattern type filter") @RequestParam(required = false) String patternType,
            @Parameter(description = "Principal filter") @RequestParam(required = false) String principal,
            @Parameter(description = "Host filter") @RequestParam(required = false) String host,
            @Parameter(description = "Operation filter") @RequestParam(required = false) String operation,
            @Parameter(description = "Permission type filter") @RequestParam(required = false) String permissionType) {
        return service.list(
                defaultClusterId,
                new AclFilterRequest(
                        resourceType, resourceName, patternType, principal, host, operation, permissionType));
    }

    @PostMapping
    @Operation(
            summary = "Create ACLs",
            description = "Create one or more ACL entries.",
            tags = {"ACLs"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "ACLs created"),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = AclCreateRequest.class)))
    })
    public ResponseEntity<Void> create(
            @Valid @org.springframework.web.bind.annotation.RequestBody AclCreateRequest request) {
        service.create(defaultClusterId, request);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/delete")
    @Operation(
            summary = "Delete ACLs",
            description = "Delete ACL entries matching the provided filter.",
            tags = {"ACLs"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "ACLs deleted"),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = AclDeleteRequest.class)))
    })
    public ResponseEntity<Void> delete(
            @Valid @org.springframework.web.bind.annotation.RequestBody AclDeleteRequest request) {
        service.delete(defaultClusterId, request);
        return ResponseEntity.noContent().build();
    }
}
