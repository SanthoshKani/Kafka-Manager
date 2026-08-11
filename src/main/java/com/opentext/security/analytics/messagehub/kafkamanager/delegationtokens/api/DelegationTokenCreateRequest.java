package com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.api;

import jakarta.validation.constraints.Min;

public record DelegationTokenCreateRequest(@Min(1) long maxLifeTimeMs) {}
