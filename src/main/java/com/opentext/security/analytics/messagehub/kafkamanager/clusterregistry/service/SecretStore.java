package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service;

import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.domain.SecretEntity;
import java.util.Optional;
import java.util.UUID;

public interface SecretStore {

    SecretEntity save(SecretEntity entity);

    Optional<SecretEntity> findById(UUID id);

    void deleteAll();
}
