package com.opentext.security.analytics.messagehub.kafkamanager.operations.domain;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Abstraction for storing operation events in-memory.
 */
public interface OperationEventStore {

    OperationEventEntity save(OperationEventEntity entity);

    OperationEventEntity saveAndFlush(OperationEventEntity entity);

    List<OperationEventEntity> findAllByOperationIdOrderBySequenceNumberAsc(UUID operationId);

    long countByOperationId(UUID operationId);

    Optional<OperationEventEntity> findById(UUID id);

    void deleteAll();
}
