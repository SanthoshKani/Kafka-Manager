package com.opentext.security.analytics.messagehub.kafkamanager.topics.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record TopicConfigMutationRequest(
        @NotBlank String name, String value, @NotNull ConfigMutationOperation operation) {}
