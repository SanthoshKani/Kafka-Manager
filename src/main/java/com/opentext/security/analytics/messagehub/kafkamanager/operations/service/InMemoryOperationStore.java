package com.opentext.security.analytics.messagehub.kafkamanager.operations.service;

import com.opentext.security.analytics.messagehub.kafkamanager.operations.domain.OperationEntity;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.domain.OperationState;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.domain.OperationStore;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

@Component
@Primary
public class InMemoryOperationStore implements OperationStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryOperationStore.class);

    private final ConcurrentMap<UUID, OperationEntity> map = new ConcurrentHashMap<>();

    @Override
    public Page<OperationEntity> findByClusterIdOrderByCreatedAtDesc(UUID clusterId, Pageable pageable) {
        List<OperationEntity> list = map.values().stream()
                .filter(e -> e.getClusterId() != null && e.getClusterId().equals(clusterId))
                .sorted(Comparator.comparing(
                        OperationEntity::getCreatedAt, Comparator.nullsLast(Comparator.reverseOrder())))
                .collect(Collectors.toList());
        int start = (int) pageable.getOffset();
        int end = Math.min(start + pageable.getPageSize(), list.size());
        List<OperationEntity> sub = start >= list.size() ? List.of() : list.subList(start, end);
        return new PageImpl<>(sub, pageable, list.size());
    }

    @Override
    public Optional<OperationEntity> findById(UUID id) {
        return Optional.ofNullable(map.get(id));
    }

    @Override
    public OperationEntity save(OperationEntity entity) {
        if (entity.getId() == null) {
            entity.setId(UUID.randomUUID());
        }
        Instant now = Instant.now();
        // Use public setters added to OperationEntity instead of reflection
        if (entity.getCreatedAt() == null) {
            entity.setCreatedAt(now);
        }
        entity.setUpdatedAt(now);
        map.put(entity.getId(), entity);
        return entity;
    }

    @Override
    public OperationEntity saveAndFlush(OperationEntity entity) {
        return save(entity);
    }

    @Override
    public void deleteAll() {
        map.clear();
    }

    @Override
    public boolean existsById(UUID id) {
        return map.containsKey(id);
    }

    @Override
    public Optional<OperationEntity> findByClusterIdAndIdempotencyKey(UUID clusterId, String idempotencyKey) {
        return map.values().stream()
                .filter(e -> e.getClusterId() != null && e.getClusterId().equals(clusterId))
                .filter(e -> idempotencyKey != null && idempotencyKey.equals(e.getIdempotencyKey()))
                .findFirst();
    }

    @Override
    public List<OperationEntity> findByCurrentStateInAndLeaseExpiresAtBefore(
            Collection<OperationState> states, Instant cutoff) {
        return map.values().stream()
                .filter(e -> e.getCurrentState() != null && states.contains(e.getCurrentState()))
                .filter(e ->
                        e.getLeaseExpiresAt() == null || e.getLeaseExpiresAt().isBefore(cutoff))
                .collect(Collectors.toList());
    }

    @Override
    public List<OperationEntity> claimable(Collection<OperationState> states, Instant cutoff) {
        return map.values().stream()
                .filter(e -> e.getCurrentState() != null && states.contains(e.getCurrentState()))
                .filter(e ->
                        e.getLeaseExpiresAt() == null || e.getLeaseExpiresAt().isBefore(cutoff))
                .sorted(Comparator.comparing(
                        OperationEntity::getCreatedAt, Comparator.nullsLast(Comparator.naturalOrder())))
                .collect(Collectors.toList());
    }
}
