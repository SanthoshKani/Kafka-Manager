package com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record ConsumerGroupOffsetUpdate(
        @NotBlank String topic,
        @Min(0) int partition,
        @Min(0) long offset) {}
