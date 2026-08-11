package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public record UpdateClusterRequest(
        String displayName,
        String description,
        String bootstrapServers,
        String controllerBootstrapEndpoints,
        String securityProtocol,
        String saslMechanism,
        String username,
        String credentialSecret,
        String truststoreSecret,
        String keystoreSecret,
        String truststorePasswordSecret,
        String keystorePasswordSecret,
        String keyPasswordSecret,
        String sslEndpointIdentificationAlgorithm,
        String sslEnabledProtocols,
        String sslTruststoreType,
        String sslKeystoreType,
        List<String> clientPropertiesAllowlist,
        String environment,
        String ownerTeam,
        List<String> tags,
        Boolean enabled,
        Duration connectionTimeout,
        Duration requestTimeout,
        Duration operationTimeout,
        Map<String, String> clientProperties,
        Long expectedVersion,
        String idempotencyKey,
        boolean dryRun) {}
