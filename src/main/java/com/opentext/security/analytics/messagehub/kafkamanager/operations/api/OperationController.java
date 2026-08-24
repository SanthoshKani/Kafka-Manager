package com.opentext.security.analytics.messagehub.kafkamanager.operations.api;

import com.opentext.security.analytics.messagehub.kafkamanager.operations.service.OperationService;
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
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/operations")
@Tag(name = "Operations", description = "Submit and query long-running operations")
@SecurityRequirement(name = "bearerAuth")
public class OperationController {

    private final OperationService operationService;
    private final java.util.UUID defaultClusterId;

    public OperationController(OperationService operationService, java.util.UUID defaultClusterId) {
        this.operationService = operationService;
        this.defaultClusterId = defaultClusterId;
    }

    @PostMapping
    @Operation(
            summary = "Submit operation",
            description = "Submit a long-running operation. Returns operation details including id.",
            tags = {"Operations"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Operation submitted",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = OperationDetailResponse.class))),
        @ApiResponse(responseCode = "400", description = "Invalid request")
    })
    public OperationDetailResponse submit(
            @Valid @org.springframework.web.bind.annotation.RequestBody SubmitOperationRequest request) {
        return operationService.submit(defaultClusterId, request);
    }

    @GetMapping
    @Operation(
            summary = "List operations",
            description = "List operations with pagination.",
            tags = {"Operations"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Paged operations list",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = OperationSummaryResponse.class)))
    })
    public Page<OperationSummaryResponse> list(
            @Parameter(description = "Page number (zero-based)") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size") @RequestParam(defaultValue = "20") int size) {
        return operationService.list(defaultClusterId, page, size);
    }

    @GetMapping("/{operationId}")
    @Operation(
            summary = "Get operation",
            description = "Get operation detail by id.",
            tags = {"Operations"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Operation detail",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = OperationDetailResponse.class))),
        @ApiResponse(responseCode = "404", description = "Operation not found")
    })
    public OperationDetailResponse get(
            @Parameter(description = "Operation id", required = true) @PathVariable UUID operationId) {
        return operationService.get(operationId);
    }

    @GetMapping("/{operationId}/events")
    @Operation(
            summary = "Get operation events",
            description = "Get events (status updates) for an operation.",
            tags = {"Operations"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "List of operation events",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = OperationEventResponse.class)))
    })
    public List<OperationEventResponse> events(
            @Parameter(description = "Operation id", required = true) @PathVariable UUID operationId) {
        return operationService.events(operationId);
    }

    @PostMapping("/{operationId}/cancel")
    @Operation(
            summary = "Cancel operation",
            description = "Request cancellation for a pending operation.",
            tags = {"Operations"})
    @ApiResponses({@ApiResponse(responseCode = "204", description = "Cancellation requested")})
    public void cancel(@Parameter(description = "Operation id", required = true) @PathVariable UUID operationId) {
        operationService.cancel(operationId);
    }

    @PostMapping("/{operationId}/retry")
    @Operation(
            summary = "Retry operation",
            description = "Retry a failed operation.",
            tags = {"Operations"})
    @ApiResponses({@ApiResponse(responseCode = "204", description = "Retry requested")})
    public void retry(@Parameter(description = "Operation id", required = true) @PathVariable UUID operationId) {
        operationService.retry(operationId);
    }
}
