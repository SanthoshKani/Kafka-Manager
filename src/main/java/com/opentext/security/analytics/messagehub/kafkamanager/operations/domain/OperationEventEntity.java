package com.opentext.security.analytics.messagehub.kafkamanager.operations.domain;

import java.time.Instant;
import java.util.UUID;

public class OperationEventEntity {

    private UUID id;

    private UUID operationId;

    private String eventType;

    private String message;

    private Instant createdAt;

    private long sequenceNumber;

    protected OperationEventEntity() {}

    public OperationEventEntity(UUID id, UUID operationId, String eventType, String message, long sequenceNumber) {
        this.id = id;
        this.operationId = operationId;
        this.eventType = eventType;
        this.message = message;
        this.sequenceNumber = sequenceNumber;
        this.createdAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public UUID getOperationId() {
        return operationId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getMessage() {
        return message;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }
}
