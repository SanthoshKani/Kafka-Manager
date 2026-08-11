package com.opentext.security.analytics.messagehub.kafkamanager.scram.api;

import jakarta.validation.constraints.NotBlank;

public record ScramCredentialDeleteRequest(@NotBlank String mechanism) {}
