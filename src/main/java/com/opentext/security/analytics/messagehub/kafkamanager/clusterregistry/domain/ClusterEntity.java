package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.domain;

import java.time.Instant;
import java.util.UUID;

public class ClusterEntity {

    private UUID id;

    private long version;

    private String displayName;

    private String description;

    private String bootstrapServers;

    private String controllerBootstrapEndpoints;

    private String securityProtocol;

    private String saslMechanism;

    private String username;

    private UUID credentialSecretId;

    private UUID truststoreSecretId;

    private UUID keystoreSecretId;

    private UUID truststorePasswordSecretId;

    private UUID keystorePasswordSecretId;

    private UUID keyPasswordSecretId;

    private String sslEndpointIdentificationAlgorithm;

    private String sslEnabledProtocols;

    private String sslTruststoreType;

    private String sslKeystoreType;

    private String clientPropertiesAllowlistJson;

    private String environment;

    private String ownerTeam;

    private String tagsJson;

    private boolean enabled;

    private long connectionTimeoutMs;

    private long requestTimeoutMs;

    private long operationTimeoutMs;

    private Instant lastSuccessfulValidationAt;

    private String lastValidationErrorSummary;

    private String observedKafkaClusterId;

    private Instant createdAt;

    private Instant updatedAt;

    public ClusterEntity() {}

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public long getVersion() {
        return version;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getControllerBootstrapEndpoints() {
        return controllerBootstrapEndpoints;
    }

    public void setControllerBootstrapEndpoints(String controllerBootstrapEndpoints) {
        this.controllerBootstrapEndpoints = controllerBootstrapEndpoints;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public UUID getCredentialSecretId() {
        return credentialSecretId;
    }

    public void setCredentialSecretId(UUID credentialSecretId) {
        this.credentialSecretId = credentialSecretId;
    }

    public UUID getTruststoreSecretId() {
        return truststoreSecretId;
    }

    public void setTruststoreSecretId(UUID truststoreSecretId) {
        this.truststoreSecretId = truststoreSecretId;
    }

    public UUID getKeystoreSecretId() {
        return keystoreSecretId;
    }

    public void setKeystoreSecretId(UUID keystoreSecretId) {
        this.keystoreSecretId = keystoreSecretId;
    }

    public UUID getTruststorePasswordSecretId() {
        return truststorePasswordSecretId;
    }

    public void setTruststorePasswordSecretId(UUID truststorePasswordSecretId) {
        this.truststorePasswordSecretId = truststorePasswordSecretId;
    }

    public UUID getKeystorePasswordSecretId() {
        return keystorePasswordSecretId;
    }

    public void setKeystorePasswordSecretId(UUID keystorePasswordSecretId) {
        this.keystorePasswordSecretId = keystorePasswordSecretId;
    }

    public UUID getKeyPasswordSecretId() {
        return keyPasswordSecretId;
    }

    public void setKeyPasswordSecretId(UUID keyPasswordSecretId) {
        this.keyPasswordSecretId = keyPasswordSecretId;
    }

    public String getSslEndpointIdentificationAlgorithm() {
        return sslEndpointIdentificationAlgorithm;
    }

    public void setSslEndpointIdentificationAlgorithm(String sslEndpointIdentificationAlgorithm) {
        this.sslEndpointIdentificationAlgorithm = sslEndpointIdentificationAlgorithm;
    }

    public String getSslEnabledProtocols() {
        return sslEnabledProtocols;
    }

    public void setSslEnabledProtocols(String sslEnabledProtocols) {
        this.sslEnabledProtocols = sslEnabledProtocols;
    }

    public String getSslTruststoreType() {
        return sslTruststoreType;
    }

    public void setSslTruststoreType(String sslTruststoreType) {
        this.sslTruststoreType = sslTruststoreType;
    }

    public String getSslKeystoreType() {
        return sslKeystoreType;
    }

    public void setSslKeystoreType(String sslKeystoreType) {
        this.sslKeystoreType = sslKeystoreType;
    }

    public String getClientPropertiesAllowlistJson() {
        return clientPropertiesAllowlistJson;
    }

    public void setClientPropertiesAllowlistJson(String clientPropertiesAllowlistJson) {
        this.clientPropertiesAllowlistJson = clientPropertiesAllowlistJson;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getOwnerTeam() {
        return ownerTeam;
    }

    public void setOwnerTeam(String ownerTeam) {
        this.ownerTeam = ownerTeam;
    }

    public String getTagsJson() {
        return tagsJson;
    }

    public void setTagsJson(String tagsJson) {
        this.tagsJson = tagsJson;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public void setConnectionTimeoutMs(long connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    public long getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(long requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public long getOperationTimeoutMs() {
        return operationTimeoutMs;
    }

    public void setOperationTimeoutMs(long operationTimeoutMs) {
        this.operationTimeoutMs = operationTimeoutMs;
    }

    public Instant getLastSuccessfulValidationAt() {
        return lastSuccessfulValidationAt;
    }

    public void setLastSuccessfulValidationAt(Instant lastSuccessfulValidationAt) {
        this.lastSuccessfulValidationAt = lastSuccessfulValidationAt;
    }

    public String getLastValidationErrorSummary() {
        return lastValidationErrorSummary;
    }

    public void setLastValidationErrorSummary(String lastValidationErrorSummary) {
        this.lastValidationErrorSummary = lastValidationErrorSummary;
    }

    public String getObservedKafkaClusterId() {
        return observedKafkaClusterId;
    }

    public void setObservedKafkaClusterId(String observedKafkaClusterId) {
        this.observedKafkaClusterId = observedKafkaClusterId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    // Added setters to allow programmatic population without reflection
    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
