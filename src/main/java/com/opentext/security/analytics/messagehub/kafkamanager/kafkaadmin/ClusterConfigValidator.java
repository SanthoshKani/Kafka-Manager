package com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin;

import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.domain.ClusterEntity;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates cluster configuration for AdminClient connectivity.
 * Ensures required security properties are present based on security protocol.
 */
public class ClusterConfigValidator {

    private static final Logger log = LoggerFactory.getLogger(ClusterConfigValidator.class);

    /**
     * Validate cluster configuration and return validation errors.
     *
     * @param cluster the cluster entity to validate
     * @return list of validation error messages (empty if valid)
     */
    public static List<String> validate(ClusterEntity cluster) {
        List<String> errors = new ArrayList<>();

        if ((cluster.getBootstrapServers() == null || cluster.getBootstrapServers().isBlank())
                && (cluster.getControllerBootstrapEndpoints() == null
                        || cluster.getControllerBootstrapEndpoints().isBlank())) {
            errors.add("bootstrapServers is required");
        }

        validateEndpointList(cluster.getBootstrapServers(), "bootstrapServers", errors);
        validateEndpointList(cluster.getControllerBootstrapEndpoints(), "controllerBootstrapEndpoints", errors);

        String securityProtocol = cluster.getSecurityProtocol();
        if (securityProtocol == null || securityProtocol.isBlank()) {
            errors.add("securityProtocol is required");
            return errors; // Can't validate further without protocol
        }

        switch (securityProtocol.toUpperCase(Locale.ROOT)) {
            case "PLAINTEXT":
                // No additional validation needed
                break;

            case "SSL":
                validateSslConfig(cluster, errors);
                break;

            case "SASL_PLAINTEXT":
                validateSaslConfig(cluster, errors);
                break;

            case "SASL_SSL":
                validateSslConfig(cluster, errors);
                validateSaslConfig(cluster, errors);
                break;

            default:
                errors.add("Invalid securityProtocol: " + securityProtocol
                        + ". Supported: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL");
        }

        return errors;
    }

    /**
     * Validate cluster configuration and throw exception if invalid.
     *
     * @param cluster the cluster entity to validate
     * @throws IllegalStateException if validation fails
     */
    public static void validateOrThrow(ClusterEntity cluster) {
        List<String> errors = validate(cluster);
        if (!errors.isEmpty()) {
            throw new IllegalStateException("Invalid cluster configuration: " + String.join(", ", errors));
        }
    }

    private static void validateSslConfig(ClusterEntity cluster, List<String> errors) {
        // Truststore is required for SSL/TLS - either for server verification or mutual TLS
        if (cluster.getTruststoreSecretId() == null) {
            errors.add("truststoreSecret is required for SSL/SASL_SSL security protocol");
        }

        // If keystore is provided, validate mutual TLS is properly configured
        if (cluster.getKeystoreSecretId() != null) {
            // Keystore password is typically required
            if (cluster.getKeystorePasswordSecretId() == null) {
                log.warn("keystoreSecret provided without keystorePasswordSecret - may cause connection issues");
            }
        }

        // Validate truststore type if specified
        String truststoreType = cluster.getSslTruststoreType();
        if (truststoreType != null && !truststoreType.isBlank()) {
            if (!isValidKeystoreType(truststoreType)) {
                errors.add("Invalid sslTruststoreType: " + truststoreType + ". Supported: JKS, PKCS12, PEM");
            }
        }

        // Validate keystore type if specified
        String keystoreType = cluster.getSslKeystoreType();
        if (keystoreType != null && !keystoreType.isBlank()) {
            if (!isValidKeystoreType(keystoreType)) {
                errors.add("Invalid sslKeystoreType: " + keystoreType + ". Supported: JKS, PKCS12, PEM");
            }
        }

        // Validate enabled protocols if specified
        String enabledProtocols = cluster.getSslEnabledProtocols();
        if (enabledProtocols != null && !enabledProtocols.isBlank()) {
            String[] protocols = enabledProtocols.split(",");
            for (String protocol : protocols) {
                String trimmed = protocol.trim();
                if (!isValidTlsProtocol(trimmed)) {
                    errors.add("Invalid SSL protocol: " + trimmed + ". Supported: TLSv1.2, TLSv1.3");
                }
            }
        }
    }

    private static void validateSaslConfig(ClusterEntity cluster, List<String> errors) {
        String saslMechanism = cluster.getSaslMechanism();
        if (saslMechanism == null || saslMechanism.isBlank()) {
            errors.add("saslMechanism is required for SASL_PLAINTEXT/SASL_SSL security protocol");
            return; // Can't validate further
        }

        // Validate mechanism is supported
        switch (saslMechanism.toUpperCase(Locale.ROOT)) {
            case "SCRAM-SHA-256":
            case "SCRAM-SHA-512":
            case "PLAIN":
                // Supported mechanisms
                break;
            case "GSSAPI":
            case "OAUTHBEARER":
                errors.add("SASL mechanism " + saslMechanism
                        + " is not currently supported. Supported: SCRAM-SHA-256, SCRAM-SHA-512, PLAIN");
                return;
            default:
                errors.add(
                        "Invalid saslMechanism: " + saslMechanism + ". Supported: SCRAM-SHA-256, SCRAM-SHA-512, PLAIN");
                return;
        }

        // Username and credential are required for SCRAM and PLAIN
        if (cluster.getUsername() == null || cluster.getUsername().isBlank()) {
            errors.add("username is required for SASL authentication");
        }

        if (cluster.getCredentialSecretId() == null) {
            errors.add("credentialSecret (password) is required for SASL authentication");
        }
    }

    private static boolean isValidKeystoreType(String type) {
        return type.equalsIgnoreCase("JKS") || type.equalsIgnoreCase("PKCS12") || type.equalsIgnoreCase("PEM");
    }

    private static boolean isValidTlsProtocol(String protocol) {
        return protocol.equals("TLSv1.2")
                || protocol.equals("TLSv1.3")
                || protocol.equals("TLSv1.1") // Deprecated but sometimes needed
                || protocol.equals("TLSv1"); // Very old, discouraged
    }

    private static void validateEndpointList(String endpoints, String fieldName, List<String> errors) {
        if (endpoints == null || endpoints.isBlank()) {
            return;
        }

        try {
            KafkaEndpointSupport.validateEndpointList(endpoints);
        } catch (IllegalArgumentException exception) {
            errors.add(fieldName + " is invalid: " + exception.getMessage());
        }
    }
}
