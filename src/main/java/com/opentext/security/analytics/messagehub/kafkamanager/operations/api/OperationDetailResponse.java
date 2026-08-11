package com.opentext.security.analytics.messagehub.kafkamanager.operations.api;

import com.opentext.security.analytics.messagehub.kafkamanager.operations.domain.OperationState;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OperationDetailResponse(
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
        String failureCategory,
        String failureDetails,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant completedAt,
        List<OperationEventResponse> events) {}
