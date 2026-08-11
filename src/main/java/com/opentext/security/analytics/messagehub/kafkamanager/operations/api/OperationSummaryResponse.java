package com.opentext.security.analytics.messagehub.kafkamanager.operations.api;

import com.opentext.security.analytics.messagehub.kafkamanager.operations.domain.OperationState;
import java.time.Instant;
import java.util.UUID;

public record OperationSummaryResponse(
        UUID id,
        UUID clusterId,
        String operationType,
        String targetResourceName,
        OperationState currentState,
        String requestedBy,
        String approvedBy,
        String idempotencyKey,
        boolean dryRun,
        int retryCount,
        boolean cancellationRequested,
        boolean cancelled,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant completedAt) {}
