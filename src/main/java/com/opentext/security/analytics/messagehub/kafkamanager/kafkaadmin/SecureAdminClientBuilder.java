package com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.Locale;
import java.util.Properties;

/**
 * Helper class to build secure Kafka AdminClient properties with support for
 * PLAINTEXT, SSL, SASL_PLAINTEXT, and SASL_SSL security protocols.
 * Handles SCRAM-SHA-256, SCRAM-SHA-512, and PLAIN SASL mechanisms.
 * Manages temporary keystore/truststore files when secrets contain base64-encoded bytes.
 */
public class SecureAdminClientBuilder {

    private static final Logger log = LoggerFactory.getLogger(SecureAdminClientBuilder.class);

    private final Properties properties = new Properties();
    private Path tempTruststoreFile;
    private Path tempKeystoreFile;

    public SecureAdminClientBuilder bootstrapServers(String bootstrapServers) {
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return this;
    }

    public SecureAdminClientBuilder clientId(String clientId) {
        properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        return this;
    }

    public SecureAdminClientBuilder requestTimeout(int timeoutMs) {
        properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        return this;
    }

    public SecureAdminClientBuilder defaultApiTimeout(int timeoutMs) {
        properties.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs);
        return this;
    }

    public SecureAdminClientBuilder connectionsMaxIdle(long idleMs) {
        properties.put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, idleMs);
        return this;
    }

    public SecureAdminClientBuilder securityProtocol(String securityProtocol) {
        if (securityProtocol == null || securityProtocol.isBlank()) {
            throw new IllegalArgumentException("securityProtocol is required");
        }
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.trim().toUpperCase(Locale.ROOT));
        return this;
    }

    /**
     * Configure SASL authentication with SCRAM or PLAIN mechanism.
     *
     * @param mechanism SCRAM-SHA-256, SCRAM-SHA-512, or PLAIN
     * @param username SASL username
     * @param password SASL password (resolved secret)
     * @return this builder
     */
    public SecureAdminClientBuilder sasl(String mechanism, String username, String password) {
        if (mechanism == null || mechanism.isBlank()) {
            return this;
        }

        String normalizedMechanism = mechanism.trim().toUpperCase(Locale.ROOT);
        properties.put(SaslConfigs.SASL_MECHANISM, normalizedMechanism);

        String jaasConfig;
        switch (normalizedMechanism) {
            case "SCRAM-SHA-256":
            case "SCRAM-SHA-512":
                jaasConfig = String.format(
                        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                        escapeJaasValue(username), escapeJaasValue(password));
                break;
            case "PLAIN":
                jaasConfig = String.format(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                        escapeJaasValue(username), escapeJaasValue(password));
                break;
            default:
                throw new IllegalArgumentException("Unsupported SASL mechanism: " + mechanism
                        + ". Supported: SCRAM-SHA-256, SCRAM-SHA-512, PLAIN");
        }

        properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        log.debug("Configured SASL mechanism: {}", mechanism);
        return this;
    }

    /**
     * Configure SSL truststore. Supports both file paths and base64-encoded keystore bytes.
     *
     * @param truststoreSecret file path or base64-encoded JKS/PKCS12 bytes
     * @param password truststore password (can be null)
     * @param type truststore type (JKS, PKCS12, etc.), defaults to JKS
     * @return this builder
     * @throws IOException if temporary file creation fails
     */
    public SecureAdminClientBuilder truststore(String truststoreSecret, String password, String type)
            throws IOException {
        if (truststoreSecret == null || truststoreSecret.isBlank()) {
            return this;
        }

        String truststorePath = resolveTruststorePath(truststoreSecret);
        properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);

        if (password != null && !password.isBlank()) {
            properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, password);
        }

        if (type != null && !type.isBlank()) {
            properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, type);
        } else {
            properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        }

        log.debug("Configured SSL truststore: {}", truststorePath);
        return this;
    }

    /**
     * Configure SSL keystore for mutual TLS. Supports both file paths and base64-encoded keystore bytes.
     *
     * @param keystoreSecret file path or base64-encoded JKS/PKCS12 bytes
     * @param keystorePassword keystore password (can be null)
     * @param keyPassword private key password (can be null)
     * @param type keystore type (JKS, PKCS12, etc.), defaults to JKS
     * @return this builder
     * @throws IOException if temporary file creation fails
     */
    public SecureAdminClientBuilder keystore(
            String keystoreSecret, String keystorePassword, String keyPassword, String type) throws IOException {
        if (keystoreSecret == null || keystoreSecret.isBlank()) {
            return this;
        }

        String keystorePath = resolveKeystorePath(keystoreSecret);
        properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystorePath);

        if (keystorePassword != null && !keystorePassword.isBlank()) {
            properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        }

        if (keyPassword != null && !keyPassword.isBlank()) {
            properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        }

        if (type != null && !type.isBlank()) {
            properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, type);
        } else {
            properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
        }

        log.debug("Configured SSL keystore: {}", keystorePath);
        return this;
    }

    /**
     * Configure SSL endpoint identification algorithm.
     * Set to empty string to disable hostname verification (NOT recommended for production).
     *
     * @param algorithm "https" (default) or "" (disabled)
     * @return this builder
     */
    public SecureAdminClientBuilder sslEndpointIdentification(String algorithm) {
        if (algorithm != null) {
            properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, algorithm);
        }
        return this;
    }

    /**
     * Configure enabled SSL/TLS protocols.
     *
     * @param protocols comma-separated list (e.g., "TLSv1.2,TLSv1.3")
     * @return this builder
     */
    public SecureAdminClientBuilder sslEnabledProtocols(String protocols) {
        if (protocols != null && !protocols.isBlank()) {
            properties.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, protocols);
        }
        return this;
    }

    public Properties build() {
        return properties;
    }

    /**
     * Clean up any temporary keystore/truststore files created during build.
     */
    public void cleanupTempFiles() {
        if (tempTruststoreFile != null) {
            try {
                Files.deleteIfExists(tempTruststoreFile);
                log.debug("Deleted temporary truststore: {}", tempTruststoreFile);
            } catch (IOException e) {
                log.warn("Failed to delete temporary truststore file: {}", tempTruststoreFile, e);
            }
        }

        if (tempKeystoreFile != null) {
            try {
                Files.deleteIfExists(tempKeystoreFile);
                log.debug("Deleted temporary keystore: {}", tempKeystoreFile);
            } catch (IOException e) {
                log.warn("Failed to delete temporary keystore file: {}", tempKeystoreFile, e);
            }
        }
    }

    private String resolveTruststorePath(String truststoreSecret) throws IOException {
        // If it's already a file path (doesn't look like base64), return as-is
        if (looksLikeFilePath(truststoreSecret)) {
            return truststoreSecret;
        }

        // Treat as base64-encoded keystore bytes - write to temp file
        byte[] keystoreBytes = Base64.getDecoder().decode(truststoreSecret);
        tempTruststoreFile = createSecureTempFile("kafka-truststore-", ".jks", keystoreBytes);
        return tempTruststoreFile.toAbsolutePath().toString();
    }

    private String resolveKeystorePath(String keystoreSecret) throws IOException {
        // If it's already a file path (doesn't look like base64), return as-is
        if (looksLikeFilePath(keystoreSecret)) {
            return keystoreSecret;
        }

        // Treat as base64-encoded keystore bytes - write to temp file
        byte[] keystoreBytes = Base64.getDecoder().decode(keystoreSecret);
        tempKeystoreFile = createSecureTempFile("kafka-keystore-", ".jks", keystoreBytes);
        return tempKeystoreFile.toAbsolutePath().toString();
    }

    private boolean looksLikeFilePath(String value) {
        // Simple heuristic: file paths typically contain / or \ and aren't pure base64
        return value.contains("/") || value.contains("\\") || value.contains(":");
    }

    private Path createSecureTempFile(String prefix, String suffix, byte[] content) throws IOException {
        Path tempFile = Files.createTempFile(prefix, suffix);

        // Set restrictive permissions (owner read/write only) on POSIX systems
        try {
            Files.setPosixFilePermissions(tempFile, PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException e) {
            // Windows - no POSIX permissions, rely on default ACLs
            log.debug("POSIX file permissions not supported on this platform");
        }

        Files.write(tempFile, content, StandardOpenOption.WRITE);
        tempFile.toFile().deleteOnExit();

        return tempFile;
    }

    private String escapeJaasValue(String value) {
        if (value == null) {
            return "";
        }
        // Escape quotes and backslashes in JAAS config values
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
