package com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin;

import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.domain.ClusterEntity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for ClusterConfigValidator.
 * Validates cluster security configurations before AdminClient creation.
 */
class ClusterConfigValidatorTest {

    private static final String PROTOCOL_PLAINTEXT = "PLAINTEXT";
    private static final String PROTOCOL_SSL = "SSL";
    private static final String PROTOCOL_SASL_PLAINTEXT = "SASL_PLAINTEXT";
    private static final String PROTOCOL_SASL_SSL = "SASL_SSL";
    private static final String MECH_SCRAM_SHA_256 = "SCRAM-SHA-256";
    private static final String MECH_SCRAM_SHA_512 = "SCRAM-SHA-512";
    private static final String MECH_PLAIN = "PLAIN";
    private static final String TRUSTSTORE_REQUIRED_MSG =
            "truststoreSecret is required for SSL/SASL_SSL security protocol";
    private static final String USER_TEST = "testuser";
    private static final String KEY_JKS = "JKS";

    @Test
    void acceptsPlaintextConfiguration() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol(PROTOCOL_PLAINTEXT);

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).isEmpty();
    }

    @Test
    void rejectsMissingSecurityProtocol() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol(null);

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).containsExactly("securityProtocol is required");
    }

    @Test
    void acceptsControllerBootstrapEndpointsWithoutBrokerBootstrapServers() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setBootstrapServers(null);
        cluster.setControllerBootstrapEndpoints("CONTROLLER://controller-1:9093,CONTROLLER://controller-2:9093");
        cluster.setSecurityProtocol(PROTOCOL_PLAINTEXT);

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).isEmpty();
    }

    @Test
    void acceptsListenerPrefixedBootstrapServers() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setBootstrapServers("BROKER://broker-1:9092, PLAINTEXT://broker-2:9092");
        cluster.setSecurityProtocol(PROTOCOL_PLAINTEXT);

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).isEmpty();
    }

    @Test
    void rejectsMalformedListenerPrefixedBootstrapServers() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setBootstrapServers("BROKER://");
        cluster.setSecurityProtocol(PROTOCOL_PLAINTEXT);

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).anyMatch(e -> e.contains("bootstrapServers is invalid"));
    }

    @Test
    void rejectsInvalidSecurityProtocol() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol("INVALID");

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).anyMatch(e -> e.contains("Invalid securityProtocol"));
    }

    @Test
    void rejectsSslWithoutTruststore() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol(PROTOCOL_SSL);
        cluster.setTruststoreSecretId(null);

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).contains(TRUSTSTORE_REQUIRED_MSG);
    }

    @Test
    void acceptsSslWithTruststore() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol(PROTOCOL_SSL);
        cluster.setTruststoreSecretId(UUID.randomUUID());
        cluster.setSslTruststoreType("JKS");

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).isEmpty();
    }

    @Test
    void acceptsSslWithMutualTls() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol(PROTOCOL_SSL);
        cluster.setTruststoreSecretId(UUID.randomUUID());
        cluster.setKeystoreSecretId(UUID.randomUUID());
        cluster.setKeystorePasswordSecretId(UUID.randomUUID());

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).isEmpty();
    }

    @Test
    void rejectsSaslPlaintextWithoutMechanism() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol(PROTOCOL_SASL_PLAINTEXT);
        cluster.setSaslMechanism(null);

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).contains("saslMechanism is required for SASL_PLAINTEXT/SASL_SSL security protocol");
    }

    @Test
    void rejectsSaslPlaintextWithoutUsername() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol("SASL_PLAINTEXT");
        cluster.setSaslMechanism(MECH_SCRAM_SHA_256);
        cluster.setUsername(null);
        cluster.setCredentialSecretId(UUID.randomUUID());

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).contains("username is required for SASL authentication");
    }

    @Test
    void rejectsSaslPlaintextWithoutCredential() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol(PROTOCOL_SASL_PLAINTEXT);
        cluster.setSaslMechanism(MECH_SCRAM_SHA_256);
        cluster.setUsername(USER_TEST);
        cluster.setCredentialSecretId(null);

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).contains("credentialSecret (password) is required for SASL authentication");
    }

    @Test
    void acceptsSaslPlaintextWithScram() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol(PROTOCOL_SASL_PLAINTEXT);
        cluster.setSaslMechanism("SCRAM-SHA-256");
        cluster.setUsername("testuser");
        cluster.setCredentialSecretId(UUID.randomUUID());

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).isEmpty();
    }

    @Test
    void acceptsSaslPlaintextWithPlain() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol("SASL_PLAINTEXT");
        cluster.setSaslMechanism(MECH_PLAIN);
        cluster.setUsername("admin");
        cluster.setCredentialSecretId(UUID.randomUUID());

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).isEmpty();
    }

    @Test
    void rejectsSaslSslWithoutTruststore() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol(PROTOCOL_SASL_SSL);
        cluster.setSaslMechanism(MECH_SCRAM_SHA_512);
        cluster.setUsername(USER_TEST);
        cluster.setCredentialSecretId(UUID.randomUUID());
        cluster.setTruststoreSecretId(null);

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).contains(TRUSTSTORE_REQUIRED_MSG);
    }

    @Test
    void acceptsSaslSslWithCompleteCon() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol(PROTOCOL_SASL_SSL);
        cluster.setSaslMechanism(MECH_SCRAM_SHA_512);
        cluster.setUsername("produser");
        cluster.setCredentialSecretId(UUID.randomUUID());
        cluster.setTruststoreSecretId(UUID.randomUUID());
        cluster.setSslTruststoreType(KEY_JKS);

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).isEmpty();
    }

    @Test
    void rejectsUnsupportedSaslMechanism() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol(PROTOCOL_SASL_PLAINTEXT);
        cluster.setSaslMechanism("GSSAPI");
        cluster.setUsername("testuser");
        cluster.setCredentialSecretId(UUID.randomUUID());

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).anyMatch(e -> e.contains("GSSAPI") && e.contains("not currently supported"));
    }

    @Test
    void rejectsInvalidTruststoreType() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol(PROTOCOL_SSL);
        cluster.setTruststoreSecretId(UUID.randomUUID());
        cluster.setSslTruststoreType("INVALID");

        List<String> errors = ClusterConfigValidator.validate(cluster);

        assertThat(errors).anyMatch(e -> e.contains("Invalid sslTruststoreType"));
    }

    @Test
    void acceptsValidTruststoreTypes() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol("SSL");
        cluster.setTruststoreSecretId(UUID.randomUUID());

        for (String type : List.of("JKS", "PKCS12", "PEM", "jks", "pkcs12", "pem")) {
            cluster.setSslTruststoreType(type);
            List<String> errors = ClusterConfigValidator.validate(cluster);
            assertThat(errors).as("TruststoreType %s should be valid", type).isEmpty();
        }
    }

    @Test
    void throwsIllegalStateExceptionWhenInvalid() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol("SSL");
        cluster.setTruststoreSecretId(null);

        assertThatThrownBy(() -> ClusterConfigValidator.validateOrThrow(cluster))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid cluster configuration")
                .hasMessageContaining("truststoreSecret is required");
    }

    @Test
    void doesNotThrowWhenValid() {
        ClusterEntity cluster = createBasicCluster();
        cluster.setSecurityProtocol(PROTOCOL_PLAINTEXT);

        ClusterConfigValidator.validateOrThrow(cluster);
        // No exception thrown
    }

    private ClusterEntity createBasicCluster() {
        ClusterEntity cluster = new ClusterEntity();
        cluster.setId(UUID.randomUUID());
        cluster.setDisplayName("Test Cluster");
        cluster.setBootstrapServers("localhost:9092");
        cluster.setEnabled(true);
        return cluster;
    }
}
