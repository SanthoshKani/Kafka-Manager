package com.opentext.security.analytics.messagehub.kafkamanager.metadataquorum.api;

import com.opentext.security.analytics.messagehub.kafkamanager.metadataquorum.service.MetadataQuorumService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/clusters/{clusterId}/metadata-quorum")
@Tag(name = "Metadata Quorum", description = "KRaft metadata quorum status and operations")
@SecurityRequirement(name = "bearerAuth")
public class MetadataQuorumController {

    private final MetadataQuorumService service;

    public MetadataQuorumController(MetadataQuorumService service) {
        this.service = service;
    }

    @GetMapping
    @Operation(
            summary = "Metadata quorum status",
            description = "Get the metadata quorum (KRaft) status for the cluster.",
            tags = {"Metadata Quorum"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Metadata quorum status",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = MetadataQuorumResponse.class)))
    })
    public MetadataQuorumResponse get(
            @Parameter(description = "Cluster UUID", required = true) @PathVariable UUID clusterId) {
        return service.get(clusterId);
    }
}
