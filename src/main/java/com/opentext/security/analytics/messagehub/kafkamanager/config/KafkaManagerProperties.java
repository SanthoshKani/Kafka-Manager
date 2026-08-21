package com.opentext.security.analytics.messagehub.kafkamanager.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "app")
public record KafkaManagerProperties(
        @NotBlank String serviceName,
        Security security,
        Admin admin,
        RateLimit rateLimit) {

    public record Security(
            BasicAuth basicAuth, OAuth2ResourceServer oauth2ResourceServer) {}

    public record BasicAuth(String username, String password) {}

    public record OAuth2ResourceServer(String issuerUri, String jwkSetUri) {}

    public record Admin(
            @NotBlank String bootstrapServers,
            @NotBlank String securityProtocol,
            Ssl ssl,
            Duration defaultRequestTimeout,
            Duration defaultOperationTimeout) {}

    public record Ssl(
            String trustStore,
            String trustStorePassword,
            String trustStoreType,
            String keyStore,
            String keyStorePassword,
            String keyStoreType,
            String keyPassword,
            String endpointIdentificationAlgorithm,
            String enabledProtocols) {}

    public record RateLimit(
            boolean enabled,
            @Min(1) int capacity,
            Duration refillPeriod,
            @NotBlank String keyHeader) {}
}
