package com.opentext.security.analytics.messagehub.kafkamanager.topics.api;

import com.opentext.security.analytics.messagehub.kafkamanager.topics.service.TopicService;
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
@RequestMapping("/api/v1/topics")
@Tag(name = "Topics", description = "Topic management: list, describe, create, delete, configs, offsets")
@SecurityRequirement(name = "bearerAuth")
public class TopicController {

    private final TopicService topicService;
    private final java.util.UUID defaultClusterId;

    public TopicController(TopicService topicService, java.util.UUID defaultClusterId) {
        this.topicService = topicService;
        this.defaultClusterId = defaultClusterId;
    }

    @GetMapping
    @Operation(
            summary = "List topics",
            description = "List topics in a cluster. Use query params to filter by prefix or include internal topics.",
            tags = {"Topics"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "List of topics",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = TopicSummaryResponse.class))),
        @ApiResponse(responseCode = "400", description = "Invalid request")
    })
    public List<TopicSummaryResponse> list(
            @Parameter(description = "Include internal topics (true|false)") @RequestParam(defaultValue = "false")
                    boolean includeInternal,
            @Parameter(description = "Topic name prefix filter") @RequestParam(required = false) String prefix) {
        return topicService.list(defaultClusterId, includeInternal, prefix);
    }

    @GetMapping("/{topicName}")
    @Operation(
            summary = "Describe topic",
            description = "Get detailed information about a topic.",
            tags = {"Topics"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Topic detail",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = TopicDetailResponse.class))),
        @ApiResponse(responseCode = "404", description = "Topic not found")
    })
    public TopicDetailResponse describe(
            @Parameter(description = "Topic name", required = true) @PathVariable String topicName) {
        return topicService.describe(defaultClusterId, topicName);
    }

    @GetMapping("/{topicName}/configs")
    @Operation(
            summary = "Topic configs",
            description = "Get topic configuration as a key/value map.",
            tags = {"Topics"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Topic configs",
                content = @Content(mediaType = "application/json")),
        @ApiResponse(responseCode = "404", description = "Topic not found")
    })
    public Map<String, String> describeConfigs(
            @Parameter(description = "Topic name", required = true) @PathVariable String topicName) {
        return topicService.describeConfigs(defaultClusterId, topicName);
    }

    @PostMapping
    @Operation(
            summary = "Create topic",
            description = "Create a new topic in the cluster.",
            tags = {"Topics"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Topic created (no content)"),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = TopicCreateRequest.class)))
    })
    public ResponseEntity<Void> create(
            @Valid @org.springframework.web.bind.annotation.RequestBody TopicCreateRequest request) throws Exception {
        topicService.create(defaultClusterId, request);
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/{topicName}")
    @Operation(
            summary = "Delete topic",
            description = "Delete a topic. Set dryRun=true to validate without deleting.",
            tags = {"Topics"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Topic deleted"),
        @ApiResponse(responseCode = "404", description = "Topic not found")
    })
    public ResponseEntity<Void> delete(
            @Parameter(description = "Topic name", required = true) @PathVariable String topicName,
            @Parameter(description = "Perform a dry-run (true|false)") @RequestParam(defaultValue = "false")
                    boolean dryRun) {
        topicService.delete(defaultClusterId, topicName, dryRun);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{topicName}/offsets")
    @Operation(
            summary = "List offsets",
            description = "List offsets for a topic. Provide timestamp when using TIMESTAMP lookup mode.",
            tags = {"Topics"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Offsets list",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = TopicOffsetResponse.class))),
        @ApiResponse(responseCode = "404", description = "Topic not found")
    })
    public List<TopicOffsetResponse> listOffsets(
            @Parameter(description = "Topic name", required = true) @PathVariable String topicName,
            @Parameter(description = "Lookup mode: EARLIEST|LATEST|TIMESTAMP") @RequestParam(defaultValue = "LATEST")
                    TopicOffsetLookupMode mode,
            @Parameter(description = "Timestamp for TIMESTAMP lookup mode (ms since epoch)")
                    @RequestParam(required = false)
                    Long timestamp) {
        return topicService.listOffsets(defaultClusterId, topicName, mode, timestamp);
    }

    @PostMapping("/{topicName}/records/delete")
    @Operation(
            summary = "Delete records",
            description = "Delete records from a topic up to specified offsets.",
            tags = {"Topics"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Records deleted"),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = TopicRecordDeleteRequest.class)))
    })
    public ResponseEntity<Void> deleteRecords(
            @Parameter(description = "Topic name", required = true) @PathVariable String topicName,
            @Valid @org.springframework.web.bind.annotation.RequestBody TopicRecordDeleteRequest request) {
        topicService.deleteRecords(defaultClusterId, topicName, request);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{topicName}/partitions")
    @Operation(
            summary = "Create partitions",
            description = "Increase the number of partitions for a topic.",
            tags = {"Topics"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Partitions scheduled"),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = TopicPartitionExpansionRequest.class)))
    })
    public ResponseEntity<Void> createPartitions(
            @Parameter(description = "Topic name", required = true) @PathVariable String topicName,
            @Valid @org.springframework.web.bind.annotation.RequestBody TopicPartitionExpansionRequest request) {
        topicService.createPartitions(defaultClusterId, topicName, request);
        return ResponseEntity.noContent().build();
    }

    @PatchMapping("/{topicName}/configs")
    @Operation(
            summary = "Alter topic configs",
            description = "Alter topic configuration using an incremental mutation batch.",
            tags = {"Topics"})
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Configs altered"),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request",
                content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = TopicConfigMutationBatchRequest.class)))
    })
    public ResponseEntity<Void> alterConfigs(
            @Parameter(description = "Topic name", required = true) @PathVariable String topicName,
            @Valid @org.springframework.web.bind.annotation.RequestBody TopicConfigMutationBatchRequest request) {
        topicService.alterConfigs(defaultClusterId, topicName, request);
        return ResponseEntity.noContent().build();
    }
}
