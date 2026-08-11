package com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.domain.ClusterEntity;
import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service.ClusterStore;
import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service.SecretStoreService;
import com.opentext.security.analytics.messagehub.kafkamanager.common.ConflictException;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AdminClientRegistry implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AdminClientRegistry.class);

    private final Cache<UUID, CachedAdminClient> cache;
    private final ClusterStore clusterStore;
    private final SecretStoreService secretStoreService;
    private final KafkaClientPropertyPolicyService policyService;
    private final KafkaManagerProperties properties;
    private final MeterRegistry meterRegistry;

    public AdminClientRegistry(
            ClusterStore clusterStore,
            SecretStoreService secretStoreService,
            KafkaClientPropertyPolicyService policyService,
            KafkaManagerProperties properties,
            MeterRegistry meterRegistry) {
        this.clusterStore = clusterStore;
        this.secretStoreService = secretStoreService;
        this.policyService = policyService;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.cache = Caffeine.newBuilder()
                .maximumSize(properties.admin().cacheSize())
                .removalListener(
                        (UUID key, CachedAdminClient value, com.github.benmanes.caffeine.cache.RemovalCause cause) -> {
                            value.close();
                        })
                .build();
    }

    public AdminClientHandle get(UUID clusterId) {
        ClusterEntity cluster = clusterStore.findById(clusterId).orElseThrow();
        String fingerprint = fingerprint(cluster);
        CachedAdminClient cached = cache.getIfPresent(clusterId);
        if (cached != null && Objects.equals(cached.fingerprint(), fingerprint)) {
            return cached.handle();
        }
        if (cached != null) {
            cache.invalidate(clusterId);
        }
        CachedAdminClient rebuilt = new CachedAdminClient(buildHandle(cluster, fingerprint), fingerprint);
        cache.put(clusterId, rebuilt);
        return rebuilt.handle();
    }

    public void invalidate(UUID clusterId) {
        cache.invalidate(clusterId);
    }

    @Override
    public void close() {
        cache.asMap().values().forEach(CachedAdminClient::close);
        cache.invalidateAll();
    }

    private AdminClientHandle buildHandle(ClusterEntity cluster, String fingerprint) {
        if (!cluster.isEnabled()) {
            throw new ConflictException("Cluster is disabled");
        }

        // Validate cluster configuration before attempting connection
        ClusterConfigValidator.validateOrThrow(cluster);

        SecureAdminClientBuilder builder = new SecureAdminClientBuilder();

        try {
            // Basic connection settings
            builder.bootstrapServers(KafkaEndpointSupport.chooseBootstrapServers(
                            cluster.getBootstrapServers(), cluster.getControllerBootstrapEndpoints()))
                    .clientId("kafka-manager-" + cluster.getId())
                    .requestTimeout(Math.toIntExact(
                            cluster.getRequestTimeoutMs() > 0
                                    ? cluster.getRequestTimeoutMs()
                                    : properties.admin().defaultRequestTimeout().toMillis()))
                    .defaultApiTimeout(Math.toIntExact(
                            cluster.getOperationTimeoutMs() > 0
                                    ? cluster.getOperationTimeoutMs()
                                    : properties
                                            .admin()
                                            .defaultOperationTimeout()
                                            .toMillis()))
                    .connectionsMaxIdle(
                            cluster.getConnectionTimeoutMs() > 0 ? cluster.getConnectionTimeoutMs() : 300000L)
                    .securityProtocol(KafkaEndpointSupport.normalizeSecurityProtocol(cluster.getSecurityProtocol()));

            // SASL configuration (for SASL_PLAINTEXT and SASL_SSL)
            if (cluster.getSaslMechanism() != null
                    && !cluster.getSaslMechanism().isBlank()) {
                String password = secretStoreService.resolve(cluster.getCredentialSecretId());
                builder.sasl(cluster.getSaslMechanism(), cluster.getUsername(), password);
            }

            // SSL configuration (for SSL and SASL_SSL)
            String securityProtocol = cluster.getSecurityProtocol();
            if ("SSL".equalsIgnoreCase(securityProtocol) || "SASL_SSL".equalsIgnoreCase(securityProtocol)) {
                // Truststore configuration (required for SSL)
                if (cluster.getTruststoreSecretId() != null) {
                    String truststoreSecret = secretStoreService.resolve(cluster.getTruststoreSecretId());
                    String truststorePassword = secretStoreService.resolve(cluster.getTruststorePasswordSecretId());
                    builder.truststore(truststoreSecret, truststorePassword, cluster.getSslTruststoreType());
                }

                // Keystore configuration (optional, for mutual TLS)
                if (cluster.getKeystoreSecretId() != null) {
                    String keystoreSecret = secretStoreService.resolve(cluster.getKeystoreSecretId());
                    String keystorePassword = secretStoreService.resolve(cluster.getKeystorePasswordSecretId());
                    String keyPassword = secretStoreService.resolve(cluster.getKeyPasswordSecretId());
                    builder.keystore(keystoreSecret, keystorePassword, keyPassword, cluster.getSslKeystoreType());
                }

                // SSL endpoint identification
                builder.sslEndpointIdentification(cluster.getSslEndpointIdentificationAlgorithm());

                // Enabled TLS protocols
                builder.sslEnabledProtocols(cluster.getSslEnabledProtocols());
            }

            Properties clientProperties = builder.build();
            Admin admin = Admin.create((Map) clientProperties);

            meterRegistry
                    .counter(
                            "kafka.manager.admin.client.created",
                            Tags.of("clusterId", cluster.getId().toString()))
                    .increment();

            return new AdminClientHandle(admin, fingerprint, builder);

        } catch (ConflictException e) {
            // Re-throw configuration conflicts as-is
            builder.cleanupTempFiles();
            throw e;
        } catch (Exception e) {
            // Clean up any temp files if admin client creation failed
            builder.cleanupTempFiles();
            log.error("Failed to create AdminClient for cluster {}: {}", cluster.getId(), e.getMessage(), e);
            throw new ConflictException("Failed to create AdminClient: " + e.getMessage());
        }
    }

    private String fingerprint(ClusterEntity cluster) {
        return cluster.getVersion()
                + ":"
                + cluster.isEnabled()
                + ":"
                + cluster.getUpdatedAt()
                + ":"
                + cluster.getSecurityProtocol()
                + ":"
                + cluster.getSaslMechanism()
                + ":"
                + cluster.getUsername()
                + ":"
                + cluster.getCredentialSecretId()
                + ":"
                + cluster.getTruststoreSecretId()
                + ":"
                + cluster.getKeystoreSecretId()
                + ":"
                + cluster.getTruststorePasswordSecretId()
                + ":"
                + cluster.getKeystorePasswordSecretId()
                + ":"
                + cluster.getKeyPasswordSecretId()
                + ":"
                + cluster.getSslEndpointIdentificationAlgorithm()
                + ":"
                + cluster.getSslEnabledProtocols()
                + ":"
                + cluster.getSslTruststoreType()
                + ":"
                + cluster.getSslKeystoreType()
                + ":"
                + cluster.getControllerBootstrapEndpoints()
                + ":"
                + cluster.getBootstrapServers();
    }

    private record CachedAdminClient(AdminClientHandle handle, String fingerprint) {
        private void close() {
            handle.close();
        }
    }

    public record AdminClientHandle(Admin admin, String fingerprint, SecureAdminClientBuilder builder)
            implements AutoCloseable {
        @Override
        public void close() {
            try {
                admin.close(Duration.ofSeconds(1));
            } finally {
                // Clean up temporary keystore/truststore files if any were created
                if (builder != null) {
                    builder.cleanupTempFiles();
                }
            }
        }
    }
}
