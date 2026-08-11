package com.opentext.security.analytics.messagehub.kafkamanager.operations.service;

import com.opentext.security.analytics.messagehub.kafkamanager.operations.domain.OperationEventEntity;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.domain.OperationEventStore;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
@Primary
public class InMemoryOperationEventStore implements OperationEventStore {

    private final ConcurrentMap<UUID, OperationEventEntity> map = new ConcurrentHashMap<>();

    @Override
    public OperationEventEntity save(OperationEventEntity entity) {
        map.put(entity.getId(), entity);
        return entity;
    }

    @Override
    public OperationEventEntity saveAndFlush(OperationEventEntity entity) {
        return save(entity);
    }

    @Override
    public List<OperationEventEntity> findAllByOperationIdOrderBySequenceNumberAsc(UUID operationId) {
        return map.values().stream()
                .filter(e -> e.getOperationId() != null && e.getOperationId().equals(operationId))
                .sorted(Comparator.comparingLong(OperationEventEntity::getSequenceNumber))
                .collect(Collectors.toList());
    }

    @Override
    public long countByOperationId(UUID operationId) {
        return map.values().stream()
                .filter(e -> e.getOperationId() != null && e.getOperationId().equals(operationId))
                .count();
    }

    @Override
    public Optional<OperationEventEntity> findById(UUID id) {
        return Optional.ofNullable(map.get(id));
    }

    @Override
    public void deleteAll() {
        map.clear();
    }
}
