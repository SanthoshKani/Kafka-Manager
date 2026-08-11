package com.opentext.security.analytics.messagehub.kafkamanager.brokers.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record BrokerConfigMutationRequest(@NotEmpty List<@Valid BrokerConfigMutationChange> changes) {}
