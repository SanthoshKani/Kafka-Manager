package com.opentext.security.analytics.messagehub.kafkamanager.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app")
public record KafkaManagerProperties(
        @NotBlank String serviceName, Security security, Admin admin, Metrics metrics, RateLimit rateLimit) {

    public record Security(BasicAuth basicAuth, OAuth2ResourceServer oauth2ResourceServer) {
        public Security {
            basicAuth = basicAuth == null ? new BasicAuth("admin", "admin") : basicAuth;
        }
    }

    public record BasicAuth(String username, String password) {}

    public record OAuth2ResourceServer(String issuerUri, String jwkSetUri) {}

    public record Admin(
            @NotBlank String bootstrapServers,
            @NotBlank String securityProtocol,
            Ssl ssl,
            Duration defaultRequestTimeout,
            Duration defaultOperationTimeout) {
        public Admin {
            bootstrapServers =
                    (bootstrapServers == null || bootstrapServers.isBlank()) ? "localhost:9092" : bootstrapServers;
            securityProtocol =
                    (securityProtocol == null || securityProtocol.isBlank()) ? "PLAINTEXT" : securityProtocol;
            defaultRequestTimeout = defaultRequestTimeout == null ? Duration.ofSeconds(5) : defaultRequestTimeout;
            defaultOperationTimeout =
                    defaultOperationTimeout == null ? Duration.ofSeconds(30) : defaultOperationTimeout;
        }
    }

    public record Metrics(BrokerJmx brokerJmx, AdminDerived adminDerived) {
        public Metrics {
            brokerJmx = brokerJmx == null ? new BrokerJmx(false, Duration.ofSeconds(30), List.of()) : brokerJmx;
            adminDerived = adminDerived == null ? new AdminDerived((Duration) null) : adminDerived;
        }

        public Metrics(AdminDerived adminDerived) {
            this(new BrokerJmx(false, Duration.ofSeconds(30), List.of()), adminDerived);
        }
    }

    public record BrokerJmx(boolean enabled, Duration pollInterval, List<BrokerJmxTarget> targets) {
        public BrokerJmx {
            targets = targets == null ? List.of() : List.copyOf(targets);
        }
    }

    public record BrokerJmxTarget(String name, String host, int port) {}

    public record AdminDerived(
            Boolean enabled, Duration pollInterval, Duration operationTimeout, List<String> topicExclusionPatterns) {

        public AdminDerived {
            enabled = enabled == null ? Boolean.TRUE : enabled;
            pollInterval = pollInterval == null ? Duration.ofSeconds(60) : pollInterval;
            operationTimeout = operationTimeout == null ? Duration.ofSeconds(30) : operationTimeout;
            topicExclusionPatterns = topicExclusionPatterns == null ? List.of() : List.copyOf(topicExclusionPatterns);

            validatePositiveDuration("app.metrics.admin-derived.poll-interval", pollInterval);
            validatePositiveDuration("app.metrics.admin-derived.operation-timeout", operationTimeout);
        }

        public AdminDerived(Duration pollInterval) {
            this(Boolean.TRUE, pollInterval, Duration.ofSeconds(30), List.of());
        }

        private static void validatePositiveDuration(String propertyName, Duration duration) {
            if (duration.compareTo(Duration.ZERO) <= 0) {
                throw new IllegalArgumentException(propertyName + " must be greater than zero");
            }
        }
    }

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
            @NotBlank String keyHeader) {
        public RateLimit {
            capacity = capacity <= 0 ? 300 : capacity;
            refillPeriod = refillPeriod == null ? Duration.ofMinutes(1) : refillPeriod;
            keyHeader = (keyHeader == null || keyHeader.isBlank()) ? "X-Client-Id" : keyHeader;
        }
    }
}
