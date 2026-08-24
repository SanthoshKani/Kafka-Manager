package com.opentext.security.analytics.messagehub.kafkamanager.operations.domain;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

/**
 * In-memory abstraction for storing operations.
 */
public interface OperationStore {

    Page<OperationEntity> findByClusterIdOrderByCreatedAtDesc(UUID clusterId, Pageable pageable);

    Optional<OperationEntity> findById(UUID id);

    OperationEntity save(OperationEntity entity);

    OperationEntity saveAndFlush(OperationEntity entity);

    void deleteAll();

    boolean existsById(UUID id);

    Optional<OperationEntity> findByClusterIdAndIdempotencyKey(UUID clusterId, String idempotencyKey);

    List<OperationEntity> findByCurrentStateInAndLeaseExpiresAtBefore(
            Collection<OperationState> states, Instant cutoff);

    /**
     * Return claimable operations ordered by createdAt ascending. Caller may update lease fields.
     */
    List<OperationEntity> claimable(Collection<OperationState> states, Instant cutoff);
}
