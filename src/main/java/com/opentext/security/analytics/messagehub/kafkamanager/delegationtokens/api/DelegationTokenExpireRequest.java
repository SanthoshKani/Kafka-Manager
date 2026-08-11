package com.opentext.security.analytics.messagehub.kafkamanager.delegationtokens.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record DelegationTokenExpireRequest(
        @NotBlank String hmacBase64, @Min(0) long expiryTimePeriodMs) {}
