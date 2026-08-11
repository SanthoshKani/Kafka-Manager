package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api.*;
import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.domain.ClusterEntity;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ConflictException;
import com.opentext.security.analytics.messagehub.kafkamanager.common.JsonSupport;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ResourceNotFoundException;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.AdminClientRegistry;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaClientPropertyPolicyService;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaEndpointSupport;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class ClusterRegistryService {

    private static final String CLUSTER_NOT_FOUND = "Cluster not found";

    private final ClusterStore clusterStore;
    private final SecretStoreService secretStoreService;
    private final KafkaAdminExecutionService adminExecutionService;
    private final AdminClientRegistry adminClientRegistry;
    private final KafkaClientPropertyPolicyService propertyPolicyService;
    private final KafkaManagerProperties properties;
    private final ObjectMapper objectMapper;

    public ClusterRegistryService(
            ClusterStore clusterStore,
            SecretStoreService secretStoreService,
            KafkaAdminExecutionService adminExecutionService,
            AdminClientRegistry adminClientRegistry,
            KafkaClientPropertyPolicyService propertyPolicyService,
            KafkaManagerProperties properties,
            ObjectMapper objectMapper) {
        this.clusterStore = clusterStore;
        this.secretStoreService = secretStoreService;
        this.adminExecutionService = adminExecutionService;
        this.adminClientRegistry = adminClientRegistry;
        this.propertyPolicyService = propertyPolicyService;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public Page<ClusterSummaryResponse> list(int page, int size) {
        Pageable pageable =
                PageRequest.of(page, Math.min(size, properties.clusterRegistry().maxPageSize()));
        return clusterStore.findAll(pageable).map(this::toSummary);
    }

    public ClusterDetailResponse get(UUID id) {
        return toDetail(clusterStore.findById(id).orElseThrow(() -> new ResourceNotFoundException(CLUSTER_NOT_FOUND)));
    }

    public ClusterDetailResponse register(RegisterClusterRequest request) {
        validateClientProperties(request.clientProperties(), request.clientPropertiesAllowlist());
        ClusterEntity entity = new ClusterEntity();
        entity.setId(UUID.randomUUID());
        apply(entity, request);
        entity.setCredentialSecretId(secretId("cluster-credential", request.credentialSecret()));
        entity.setTruststoreSecretId(secretId("cluster-truststore", request.truststoreSecret()));
        entity.setKeystoreSecretId(secretId("cluster-keystore", request.keystoreSecret()));
        entity.setTruststorePasswordSecretId(
                secretId("cluster-truststore-password", request.truststorePasswordSecret()));
        entity.setKeystorePasswordSecretId(secretId("cluster-keystore-password", request.keystorePasswordSecret()));
        entity.setKeyPasswordSecretId(secretId("cluster-key-password", request.keyPasswordSecret()));
        if (request.enabled()) {
            entity.setEnabled(true);
        }
        if (entity.getObservedKafkaClusterId() != null
                && clusterStore.existsByObservedKafkaClusterIdAndIdNot(
                        entity.getObservedKafkaClusterId(), entity.getId())) {
            throw new ConflictException("Another cluster already points to the same Kafka cluster");
        }
        clusterStore.save(entity);
        adminClientRegistry.invalidate(entity.getId());
        return toDetail(entity);
    }

    public ClusterDetailResponse update(UUID id, UpdateClusterRequest request) {
        ClusterEntity entity =
                clusterStore.findById(id).orElseThrow(() -> new ResourceNotFoundException(CLUSTER_NOT_FOUND));
        if (request.expectedVersion() != null && request.expectedVersion() != entity.getVersion()) {
            throw new ConflictException("Cluster version mismatch");
        }
        validateClientProperties(
                request.clientProperties(),
                request.clientPropertiesAllowlist() == null ? List.of() : request.clientPropertiesAllowlist());
        apply(entity, request);
        if (request.credentialSecret() != null) {
            entity.setCredentialSecretId(secretId("cluster-credential", request.credentialSecret()));
        }
        if (request.truststoreSecret() != null) {
            entity.setTruststoreSecretId(secretId("cluster-truststore", request.truststoreSecret()));
        }
        if (request.keystoreSecret() != null) {
            entity.setKeystoreSecretId(secretId("cluster-keystore", request.keystoreSecret()));
        }
        if (request.truststorePasswordSecret() != null) {
            entity.setTruststorePasswordSecretId(
                    secretId("cluster-truststore-password", request.truststorePasswordSecret()));
        }
        if (request.keystorePasswordSecret() != null) {
            entity.setKeystorePasswordSecretId(secretId("cluster-keystore-password", request.keystorePasswordSecret()));
        }
        if (request.keyPasswordSecret() != null) {
            entity.setKeyPasswordSecretId(secretId("cluster-key-password", request.keyPasswordSecret()));
        }
        adminClientRegistry.invalidate(entity.getId());
        return toDetail(entity);
    }

    public void enable(UUID id) {
        ClusterEntity entity =
                clusterStore.findById(id).orElseThrow(() -> new ResourceNotFoundException(CLUSTER_NOT_FOUND));
        entity.setEnabled(true);
        clusterStore.save(entity);
        adminClientRegistry.invalidate(id);
    }

    public void disable(UUID id) {
        ClusterEntity entity =
                clusterStore.findById(id).orElseThrow(() -> new ResourceNotFoundException(CLUSTER_NOT_FOUND));
        entity.setEnabled(false);
        clusterStore.save(entity);
        adminClientRegistry.invalidate(id);
    }

    public void delete(UUID id) {
        ClusterEntity entity =
                clusterStore.findById(id).orElseThrow(() -> new ResourceNotFoundException(CLUSTER_NOT_FOUND));
        clusterStore.delete(entity);
        adminClientRegistry.invalidate(id);
    }

    public ClusterValidationResponse validateSavedCluster(UUID id) {
        ClusterEntity entity =
                clusterStore.findById(id).orElseThrow(() -> new ResourceNotFoundException(CLUSTER_NOT_FOUND));
        ClusterValidationResponse response = validate(entity);
        clusterStore.save(entity);
        return response;
    }

    public ClusterValidationResponse validateUnsaved(RegisterClusterRequest request) {
        ClusterEntity temp = new ClusterEntity();
        temp.setId(UUID.randomUUID());
        apply(temp, request);
        temp.setCredentialSecretId(null);
        temp.setTruststoreSecretId(null);
        temp.setKeystoreSecretId(null);
        temp.setTruststorePasswordSecretId(null);
        temp.setKeystorePasswordSecretId(null);
        temp.setKeyPasswordSecretId(null);
        return validate(temp);
    }

    public CapabilityReportResponse capabilityReport(UUID id) {
        ClusterEntity entity =
                clusterStore.findById(id).orElseThrow(() -> new ResourceNotFoundException(CLUSTER_NOT_FOUND));
        return adminExecutionService.execute(
                id,
                "capability-report",
                Duration.ofMillis(
                        entity.getOperationTimeoutMs() > 0
                                ? entity.getOperationTimeoutMs()
                                : properties.admin().defaultOperationTimeout().toMillis()),
                handle -> {
                    Admin admin = handle.admin();
                    List<String> limitations = new java.util.ArrayList<>();
                    var describeCluster = admin.describeCluster();
                    Duration timeout = Duration.ofMillis(
                            entity.getOperationTimeoutMs() > 0
                                    ? entity.getOperationTimeoutMs()
                                    : properties
                                            .admin()
                                            .defaultOperationTimeout()
                                            .toMillis());
                    String clusterId = safeGet(
                            id,
                            "cluster-id",
                            timeout,
                            describeCluster.clusterId(),
                            limitations,
                            "clusterId unavailable");
                    Node controllerNode =
                            safeGet(id, "cluster-controller", timeout, describeCluster.controller(), limitations, null);
                    String controller = controllerNode == null
                            ? "controller unavailable"
                            : controllerNode.id() + "@" + controllerNode.host() + ":" + controllerNode.port();
                    List<String> brokers =
                            safeGet(id, "cluster-nodes", timeout, describeCluster.nodes(), limitations, List.of())
                                    .stream()
                                    .map(node -> node.id() + ":" + node.host() + ":" + node.port())
                                    .toList();
                    List<String> featureLevels = new java.util.ArrayList<>();
                    List<String> finalizedFeatures = new java.util.ArrayList<>();
                    List<String> metadataQuorum = new java.util.ArrayList<>();
                    try {
                        adminExecutionService.await(
                                id,
                                "feature-metadata",
                                timeout,
                                admin.describeFeatures().featureMetadata());
                        featureLevels.add("supported");
                    } catch (Exception ignored) {
                        limitations.add(
                                "Feature metadata is not available from the selected Kafka client or broker version");
                    }
                    try {
                        adminExecutionService.await(
                                id,
                                "metadata-quorum",
                                timeout,
                                admin.describeMetadataQuorum().quorumInfo());
                        metadataQuorum.add("supported");
                    } catch (UnsupportedVersionException exception) {
                        limitations.add("Metadata quorum is not supported by the connected cluster or client version");
                    } catch (Exception exception) {
                        limitations.add("Metadata quorum could not be retrieved");
                    }
                    return new CapabilityReportResponse(
                            clusterId,
                            controller,
                            brokers,
                            featureLevels,
                            finalizedFeatures,
                            metadataQuorum,
                            limitations);
                });
    }

    private ClusterValidationResponse validate(ClusterEntity entity) {
        Duration timeout = Duration.ofMillis(
                entity.getOperationTimeoutMs() > 0
                        ? entity.getOperationTimeoutMs()
                        : properties.admin().defaultOperationTimeout().toMillis());
        return adminExecutionService.execute(entity.getId(), "validate-cluster", timeout, handle -> {
            Admin admin = handle.admin();
            List<String> limitations = new java.util.ArrayList<>();
            var describeCluster = admin.describeCluster();
            String clusterId = safeGet(
                    entity.getId(),
                    "cluster-id",
                    timeout,
                    describeCluster.clusterId(),
                    limitations,
                    "clusterId unavailable");
            List<String> nodes =
                    safeGet(entity.getId(), "cluster-nodes", timeout, describeCluster.nodes(), limitations, List.of())
                            .stream()
                            .map(node -> node.id() + "@" + node.host() + ":" + node.port())
                            .toList();
            Node controllerNode = safeGet(
                    entity.getId(), "cluster-controller", timeout, describeCluster.controller(), limitations, null);
            String controller = controllerNode == null
                    ? null
                    : controllerNode.id() + "@" + controllerNode.host() + ":" + controllerNode.port();
            if (entity.getObservedKafkaClusterId() != null
                    && !entity.getObservedKafkaClusterId().equals(clusterId)
                    && clusterStore.existsByObservedKafkaClusterIdAndIdNot(
                            entity.getObservedKafkaClusterId(), entity.getId())) {
                throw new ConflictException("Another saved cluster already maps to this Kafka cluster ID");
            }
            entity.setObservedKafkaClusterId(clusterId);
            entity.setLastSuccessfulValidationAt(Instant.now());
            entity.setLastValidationErrorSummary(null);
            return new ClusterValidationResponse(
                    true,
                    clusterId,
                    controller,
                    nodes,
                    List.of(),
                    List.of("describeCluster", "brokerMetadata"),
                    null,
                    null);
        });
    }

    private void validateClientProperties(Map<String, String> clientProperties, List<String> allowlist) {
        propertyPolicyService.validate(clientProperties, allowlist == null ? List.of() : allowlist);
    }

    private UUID secretId(String purpose, String secret) {
        if (secret == null || secret.isBlank()) {
            return null;
        }
        return secretStoreService.store(purpose, secret);
    }

    private void apply(ClusterEntity entity, RegisterClusterRequest request) {
        entity.setDisplayName(request.displayName());
        entity.setDescription(request.description());
        entity.setBootstrapServers(KafkaEndpointSupport.normalizeEndpointList(request.bootstrapServers()));
        entity.setControllerBootstrapEndpoints(
                KafkaEndpointSupport.normalizeEndpointList(request.controllerBootstrapEndpoints()));
        entity.setSecurityProtocol(KafkaEndpointSupport.normalizeSecurityProtocol(request.securityProtocol()));
        entity.setSaslMechanism(request.saslMechanism());
        entity.setUsername(request.username());
        entity.setClientPropertiesAllowlistJson(JsonSupport.toJson(objectMapper, request.clientPropertiesAllowlist()));
        entity.setEnvironment(request.environment());
        entity.setOwnerTeam(request.ownerTeam());
        entity.setTagsJson(JsonSupport.toJson(objectMapper, request.tags()));
        entity.setEnabled(request.enabled());
        entity.setConnectionTimeoutMs(
                request.connectionTimeout() == null
                        ? 0
                        : request.connectionTimeout().toMillis());
        entity.setRequestTimeoutMs(
                request.requestTimeout() == null ? 0 : request.requestTimeout().toMillis());
        entity.setOperationTimeoutMs(
                request.operationTimeout() == null
                        ? 0
                        : request.operationTimeout().toMillis());
        entity.setSslEndpointIdentificationAlgorithm(request.sslEndpointIdentificationAlgorithm());
        entity.setSslEnabledProtocols(request.sslEnabledProtocols());
        entity.setSslTruststoreType(request.sslTruststoreType());
        entity.setSslKeystoreType(request.sslKeystoreType());
    }

    private void apply(ClusterEntity entity, UpdateClusterRequest request) {
        if (request.displayName() != null) {
            entity.setDisplayName(request.displayName());
        }
        if (request.description() != null) {
            entity.setDescription(request.description());
        }
        if (request.bootstrapServers() != null) {
            entity.setBootstrapServers(KafkaEndpointSupport.normalizeEndpointList(request.bootstrapServers()));
        }
        if (request.controllerBootstrapEndpoints() != null) {
            entity.setControllerBootstrapEndpoints(
                    KafkaEndpointSupport.normalizeEndpointList(request.controllerBootstrapEndpoints()));
        }
        if (request.securityProtocol() != null) {
            entity.setSecurityProtocol(KafkaEndpointSupport.normalizeSecurityProtocol(request.securityProtocol()));
        }
        if (request.saslMechanism() != null) {
            entity.setSaslMechanism(request.saslMechanism());
        }
        if (request.username() != null) {
            entity.setUsername(request.username());
        }
        if (request.clientPropertiesAllowlist() != null) {
            entity.setClientPropertiesAllowlistJson(
                    JsonSupport.toJson(objectMapper, request.clientPropertiesAllowlist()));
        }
        if (request.environment() != null) {
            entity.setEnvironment(request.environment());
        }
        if (request.ownerTeam() != null) {
            entity.setOwnerTeam(request.ownerTeam());
        }
        if (request.tags() != null) {
            entity.setTagsJson(JsonSupport.toJson(objectMapper, request.tags()));
        }
        if (request.enabled() != null) {
            entity.setEnabled(request.enabled());
        }
        if (request.connectionTimeout() != null) {
            entity.setConnectionTimeoutMs(request.connectionTimeout().toMillis());
        }
        if (request.requestTimeout() != null) {
            entity.setRequestTimeoutMs(request.requestTimeout().toMillis());
        }
        if (request.operationTimeout() != null) {
            entity.setOperationTimeoutMs(request.operationTimeout().toMillis());
        }
        if (request.sslEndpointIdentificationAlgorithm() != null) {
            entity.setSslEndpointIdentificationAlgorithm(request.sslEndpointIdentificationAlgorithm());
        }
        if (request.sslEnabledProtocols() != null) {
            entity.setSslEnabledProtocols(request.sslEnabledProtocols());
        }
        if (request.sslTruststoreType() != null) {
            entity.setSslTruststoreType(request.sslTruststoreType());
        }
        if (request.sslKeystoreType() != null) {
            entity.setSslKeystoreType(request.sslKeystoreType());
        }
    }

    private ClusterSummaryResponse toSummary(ClusterEntity entity) {
        return new ClusterSummaryResponse(
                entity.getId(),
                entity.getDisplayName(),
                entity.getDescription(),
                entity.getBootstrapServers(),
                entity.isEnabled(),
                entity.getEnvironment(),
                entity.getOwnerTeam(),
                JsonSupport.toStringList(objectMapper, entity.getTagsJson()),
                entity.getLastSuccessfulValidationAt(),
                entity.getLastValidationErrorSummary(),
                entity.getObservedKafkaClusterId(),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                entity.getVersion());
    }

    private ClusterDetailResponse toDetail(ClusterEntity entity) {
        return new ClusterDetailResponse(
                entity.getId(),
                entity.getDisplayName(),
                entity.getDescription(),
                entity.getBootstrapServers(),
                entity.getControllerBootstrapEndpoints(),
                entity.getSecurityProtocol(),
                entity.getSaslMechanism(),
                entity.getUsername(),
                entity.getCredentialSecretId(),
                entity.getTruststoreSecretId(),
                entity.getKeystoreSecretId(),
                entity.getTruststorePasswordSecretId(),
                entity.getKeystorePasswordSecretId(),
                entity.getKeyPasswordSecretId(),
                entity.getSslEndpointIdentificationAlgorithm(),
                entity.getSslEnabledProtocols(),
                entity.getSslTruststoreType(),
                entity.getSslKeystoreType(),
                JsonSupport.toStringList(objectMapper, entity.getClientPropertiesAllowlistJson()),
                entity.getEnvironment(),
                entity.getOwnerTeam(),
                JsonSupport.toStringList(objectMapper, entity.getTagsJson()),
                entity.isEnabled(),
                entity.getConnectionTimeoutMs(),
                entity.getRequestTimeoutMs(),
                entity.getOperationTimeoutMs(),
                entity.getLastSuccessfulValidationAt(),
                entity.getLastValidationErrorSummary(),
                entity.getObservedKafkaClusterId(),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                entity.getVersion());
    }

    private <T> T safeGet(
            UUID clusterId,
            String action,
            Duration timeout,
            KafkaFuture<T> future,
            List<String> limitations,
            T fallback) {
        try {
            return adminExecutionService.await(clusterId, action, timeout, future);
        } catch (Exception exception) {
            if (limitations != null) {
                limitations.add("A Kafka capability could not be fetched: "
                        + exception.getClass().getSimpleName());
            }
            return fallback;
        }
    }
}
