package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service;

import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.domain.ClusterEntity;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

/**
 * Abstraction for cluster storage. Implementations may persist to a database or keep clusters in-memory.
 */
public interface ClusterStore {

    Page<ClusterEntity> findAll(Pageable pageable);

    Optional<ClusterEntity> findById(UUID id);

    ClusterEntity save(ClusterEntity entity);

    void delete(ClusterEntity entity);

    boolean existsByObservedKafkaClusterIdAndIdNot(String observedKafkaClusterId, UUID id);

    Optional<ClusterEntity> findByObservedKafkaClusterId(String observedKafkaClusterId);

    boolean existsById(UUID id);

    void deleteAll();
}
