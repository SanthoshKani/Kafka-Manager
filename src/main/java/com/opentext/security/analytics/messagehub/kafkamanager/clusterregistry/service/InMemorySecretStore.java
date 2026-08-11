package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service;

import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.domain.SecretEntity;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
@Primary
public class InMemorySecretStore implements SecretStore {

    private final ConcurrentMap<UUID, SecretEntity> map = new ConcurrentHashMap<>();

    @Override
    public SecretEntity save(SecretEntity entity) {
        map.put(entity.getId(), entity);
        return entity;
    }

    @Override
    public Optional<SecretEntity> findById(UUID id) {
        return Optional.ofNullable(map.get(id));
    }

    @Override
    public void deleteAll() {
        map.clear();
    }
}
