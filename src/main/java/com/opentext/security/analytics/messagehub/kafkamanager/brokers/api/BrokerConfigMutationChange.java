package com.opentext.security.analytics.messagehub.kafkamanager.brokers.api;

import com.opentext.security.analytics.messagehub.kafkamanager.topics.api.ConfigMutationOperation;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record BrokerConfigMutationChange(
        @NotBlank String name, String value, @NotNull ConfigMutationOperation operation) {}
