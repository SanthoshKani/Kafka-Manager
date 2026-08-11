package com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.api;

import jakarta.validation.constraints.NotBlank;

public record DelegationTokenRenewRequest(@NotBlank String hmacBase64) {}
