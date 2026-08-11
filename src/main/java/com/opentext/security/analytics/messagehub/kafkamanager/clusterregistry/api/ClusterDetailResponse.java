package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record ClusterDetailResponse(
        UUID id,
        String displayName,
        String description,
        String bootstrapServers,
        String controllerBootstrapEndpoints,
        String securityProtocol,
        String saslMechanism,
        String username,
        UUID credentialSecretId,
        UUID truststoreSecretId,
        UUID keystoreSecretId,
        UUID truststorePasswordSecretId,
        UUID keystorePasswordSecretId,
        UUID keyPasswordSecretId,
        String sslEndpointIdentificationAlgorithm,
        String sslEnabledProtocols,
        String sslTruststoreType,
        String sslKeystoreType,
        List<String> clientPropertiesAllowlist,
        String environment,
        String ownerTeam,
        List<String> tags,
        boolean enabled,
        long connectionTimeoutMs,
        long requestTimeoutMs,
        long operationTimeoutMs,
        Instant lastSuccessfulValidationAt,
        String lastValidationErrorSummary,
        String observedKafkaClusterId,
        Instant createdAt,
        Instant updatedAt,
        long version) {}
