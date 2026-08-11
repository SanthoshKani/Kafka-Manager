package com.opentext.security.analytics.messagehub.kafkamanager.scram.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record ScramCredentialUpsertRequest(
        @NotBlank String mechanism,
        @Min(1) int iterations,
        @NotBlank String password) {}
