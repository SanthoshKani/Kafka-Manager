package com.opentext.security.analytics.messagehub.kafkamanager.topics.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.Map;

public record TopicCreateRequest(
        @NotBlank String topicName,
        @Min(1) int partitions,
        @Min(1) short replicationFactor,
        @NotNull Map<String, String> configs) {}
