package com.opentext.security.analytics.messagehub.kafkamanager.operations.api;

import java.time.Instant;
import java.util.UUID;

public record OperationEventResponse(
        UUID id, UUID operationId, String eventType, String message, Instant createdAt, long sequenceNumber) {}
