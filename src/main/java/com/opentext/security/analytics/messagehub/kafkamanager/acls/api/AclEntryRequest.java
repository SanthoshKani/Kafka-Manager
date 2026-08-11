package com.opentext.security.analytics.messagehub.kafkamanager.acls.api;

import jakarta.validation.constraints.NotBlank;

public record AclEntryRequest(
        @NotBlank String resourceType,
        String resourceName,
        @NotBlank String patternType,
        @NotBlank String principal,
        @NotBlank String host,
        @NotBlank String operation,
        @NotBlank String permissionType) {}
