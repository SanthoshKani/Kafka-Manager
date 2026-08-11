package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public record RegisterClusterRequest(
        @NotBlank String displayName,
        String description,
        @NotBlank String bootstrapServers,
        String controllerBootstrapEndpoints,
        @NotBlank String securityProtocol,
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
        @NotEmpty List<String> clientPropertiesAllowlist,
        String environment,
        String ownerTeam,
        List<String> tags,
        boolean enabled,
        Duration connectionTimeout,
        Duration requestTimeout,
        Duration operationTimeout,
        Map<String, String> clientProperties,
        String idempotencyKey,
        boolean dryRun) {}
