package com.opentext.security.analytics.messagehub.kafkamanager.operations.api;

import jakarta.validation.constraints.NotBlank;
import java.util.Map;

public record SubmitOperationRequest(
        @NotBlank String operationType,
        boolean dryRun,
        String requestedBy,
        String approvedBy,
        String idempotencyKey,
        String resourceName,
        Map<String, Object> payload) {}
