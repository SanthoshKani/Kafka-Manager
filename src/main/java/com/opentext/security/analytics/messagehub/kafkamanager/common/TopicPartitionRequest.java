package com.opentext.security.analytics.messagehub.kafkamanager.common;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record TopicPartitionRequest(
        @NotBlank String topic, @Min(0) int partition) {}
