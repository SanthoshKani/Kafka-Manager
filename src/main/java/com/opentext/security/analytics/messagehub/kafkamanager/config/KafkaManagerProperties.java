package com.opentext.security.analytics.messagehub.kafkamanager.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app")
public record KafkaManagerProperties(
        @NotBlank String serviceName,
        Security security,
        Admin admin,
        Operations operations,
        ClusterRegistry clusterRegistry,
        RateLimit rateLimit) {

    public record Security(
            @NotBlank String masterKeyBase64, BasicAuth basicAuth, OAuth2ResourceServer oauth2ResourceServer) {}

    public record BasicAuth(String username, String password) {}

    public record OAuth2ResourceServer(String issuerUri, String jwkSetUri) {}

    public record Admin(
            @Min(1) int cacheSize,
            Duration defaultRequestTimeout,
            Duration defaultOperationTimeout,
            Duration connectionValidationTimeout) {}

    public record Operations(Duration pollInterval, Duration leaseDuration) {}

    public record ClusterRegistry(@Min(1) int maxPageSize, int maxClientProperties) {}

    public record RateLimit(
            boolean enabled,
            @Min(1) int capacity,
            Duration refillPeriod,
            @NotBlank String keyHeader) {}
}
