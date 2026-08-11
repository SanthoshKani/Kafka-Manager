package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.domain.ClusterEntity;
import com.opentext.security.analytics.messagehub.kafkamanager.common.JsonSupport;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.ClusterConfigValidator;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaEndpointSupport;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple in-memory implementation of ClusterStore. Populates initial clusters from JSON provided via
 * the environment variable/property `KAFKA_MANAGER_CLUSTERS` (JSON array of cluster configuration objects).
 */
@Component
@Primary
public class InMemoryClusterStore implements ClusterStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryClusterStore.class);

    private final ConcurrentMap<UUID, ClusterEntity> map = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;

    // JSON array of cluster definitions provided via env or property. Optional.
    @Value("${KAFKA_MANAGER_CLUSTERS:}")
    private String clustersJson;

    public InMemoryClusterStore(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void initFromEnv() {
        if (clustersJson == null || clustersJson.isBlank()) {
            return;
        }
        try {
            List<Map<String, Object>> list = objectMapper.readValue(clustersJson, new TypeReference<>() {});
            int loadedCount = 0;
            int skippedCount = 0;
            for (Map<String, Object> m : list) {
                ClusterEntity e = new ClusterEntity();
                UUID id = uuidValue(m, "id");
                if (id == null) {
                    id = UUID.randomUUID();
                }
                e.setId(id);
                e.setDisplayName(defaultStringValue(m, "", "displayName"));
                e.setDescription(defaultStringValue(m, "", "description"));
                e.setBootstrapServers(
                        KafkaEndpointSupport.normalizeEndpointList(defaultStringValue(m, "", "bootstrapServers")));
                e.setControllerBootstrapEndpoints(
                        KafkaEndpointSupport.normalizeEndpointList(stringValue(m, "controllerBootstrapEndpoints")));
                String securityProtocol = stringValue(m, "securityProtocol");
                e.setSecurityProtocol(KafkaEndpointSupport.normalizeSecurityProtocol(
                        securityProtocol == null ? "PLAINTEXT" : securityProtocol));
                e.setSaslMechanism(stringValue(m, "saslMechanism"));
                e.setUsername(stringValue(m, "username"));
                e.setCredentialSecretId(uuidValue(m, "credentialSecretId"));
                e.setTruststoreSecretId(uuidValue(m, "truststoreSecretId"));
                e.setKeystoreSecretId(uuidValue(m, "keystoreSecretId"));
                e.setTruststorePasswordSecretId(uuidValue(m, "truststorePasswordSecretId"));
                e.setKeystorePasswordSecretId(uuidValue(m, "keystorePasswordSecretId"));
                e.setKeyPasswordSecretId(uuidValue(m, "keyPasswordSecretId"));
                e.setSslEndpointIdentificationAlgorithm(stringValue(m, "sslEndpointIdentificationAlgorithm"));
                e.setSslEnabledProtocols(stringValue(m, "sslEnabledProtocols"));
                e.setSslTruststoreType(stringValue(m, "sslTruststoreType"));
                e.setSslKeystoreType(stringValue(m, "sslKeystoreType"));
                e.setClientPropertiesAllowlistJson(
                        jsonArrayValue(m, "[]", "clientPropertiesAllowlistJson", "clientPropertiesAllowlist"));
                e.setEnvironment(stringValue(m, "environment"));
                e.setOwnerTeam(stringValue(m, "ownerTeam"));
                e.setTagsJson(jsonArrayValue(m, "[]", "tagsJson", "tags"));
                e.setEnabled(booleanValue(m, "enabled", true));
                e.setConnectionTimeoutMs(longValue(m, "connectionTimeoutMs", 0));
                e.setRequestTimeoutMs(longValue(m, "requestTimeoutMs", 0));
                e.setOperationTimeoutMs(longValue(m, "operationTimeoutMs", 0));
                e.setObservedKafkaClusterId(stringValue(m, "observedKafkaClusterId"));
                e.setLastValidationErrorSummary(stringValue(m, "lastValidationErrorSummary"));
                Instant now = Instant.now();
                // Use public setters (added to ClusterEntity) instead of reflection
                e.setCreatedAt(now);
                e.setUpdatedAt(now);
                List<String> validationErrors = ClusterConfigValidator.validate(e);
                if (!validationErrors.isEmpty()) {
                    skippedCount++;
                    log.warn("Skipping invalid cluster bootstrap entry {}: {}",
                            e.getDisplayName() == null || e.getDisplayName().isBlank() ? id : e.getDisplayName(),
                            String.join(", ", validationErrors));
                    continue;
                }
                map.put(id, e);
                loadedCount++;
            }
            log.info("Loaded {} clusters from environment variable KAFKA_MANAGER_CLUSTERS", loadedCount);
            if (skippedCount > 0) {
                log.warn("Skipped {} invalid cluster bootstrap entries from environment variable KAFKA_MANAGER_CLUSTERS", skippedCount);
            }
        } catch (Exception e) {
            log.error("Failed to parse KAFKA_MANAGER_CLUSTERS JSON: {}", e.getMessage());
        }
    }

    private Object firstValue(Map<String, Object> source, String... keys) {
        for (String key : keys) {
            if (source.containsKey(key)) {
                Object value = source.get(key);
                if (value != null) {
                    return value;
                }
            }
        }
        return null;
    }

    private String stringValue(Map<String, Object> source, String... keys) {
        Object value = firstValue(source, keys);
        return value == null ? null : value.toString();
    }

    private String defaultStringValue(Map<String, Object> source, String defaultValue, String... keys) {
        String value = stringValue(source, keys);
        return value == null ? defaultValue : value;
    }

    private UUID uuidValue(Map<String, Object> source, String... keys) {
        Object value = firstValue(source, keys);
        if (value == null) {
            return null;
        }
        if (value instanceof UUID uuid) {
            return uuid;
        }
        return UUID.fromString(value.toString());
    }

    private boolean booleanValue(Map<String, Object> source, String key, boolean defaultValue) {
        Object value = firstValue(source, key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private long longValue(Map<String, Object> source, String key, long defaultValue) {
        Object value = firstValue(source, key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(value.toString());
    }

    private String jsonArrayValue(Map<String, Object> source, String defaultValue, String... keys) {
        Object value = firstValue(source, keys);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof String stringValue) {
            return stringValue;
        }
        return JsonSupport.toJson(objectMapper, value);
    }

    @Override
    public Page<ClusterEntity> findAll(Pageable pageable) {
        List<ClusterEntity> list = new ArrayList<>(map.values());
        list.sort(Comparator.comparing(ClusterEntity::getCreatedAt, Comparator.nullsLast(Comparator.naturalOrder())));
        int start = (int) pageable.getOffset();
        int end = Math.min(start + pageable.getPageSize(), list.size());
        List<ClusterEntity> sub = start >= list.size() ? List.of() : list.subList(start, end);
        return new PageImpl<>(sub, pageable, list.size());
    }

    @Override
    public Optional<ClusterEntity> findById(UUID id) {
        return Optional.ofNullable(map.get(id));
    }

    @Override
    public ClusterEntity save(ClusterEntity entity) {
        if (entity.getId() == null) {
            entity.setId(UUID.randomUUID());
        }
        Instant now = Instant.now();
        if (entity.getCreatedAt() == null) {
            entity.setCreatedAt(now);
        }
        entity.setUpdatedAt(now);
        map.put(entity.getId(), entity);
        return entity;
    }

    @Override
    public void delete(ClusterEntity entity) {
        if (entity != null && entity.getId() != null) {
            map.remove(entity.getId());
        }
    }

    @Override
    public boolean existsByObservedKafkaClusterIdAndIdNot(String observedKafkaClusterId, UUID id) {
        if (observedKafkaClusterId == null) return false;
        return map.values().stream()
                .anyMatch(e -> observedKafkaClusterId.equals(e.getObservedKafkaClusterId())
                        && !e.getId().equals(id));
    }

    @Override
    public Optional<ClusterEntity> findByObservedKafkaClusterId(String observedKafkaClusterId) {
        if (observedKafkaClusterId == null) return Optional.empty();
        return map.values().stream()
                .filter(e -> observedKafkaClusterId.equals(e.getObservedKafkaClusterId()))
                .findFirst();
    }

    @Override
    public boolean existsById(UUID id) {
        return map.containsKey(id);
    }

    @Override
    public void deleteAll() {
        map.clear();
    }
}
