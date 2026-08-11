package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.domain;

import java.time.Instant;
import java.util.UUID;

public class SecretEntity {

    private UUID id;

    private String purpose;

    private String ciphertext;

    private String algorithm;

    private Instant createdAt;

    private Instant updatedAt;

    protected SecretEntity() {}

    public SecretEntity(UUID id, String purpose, String ciphertext, String algorithm) {
        this.id = id;
        this.purpose = purpose;
        this.ciphertext = ciphertext;
        this.algorithm = algorithm;
    }

    public UUID getId() {
        return id;
    }

    public String getPurpose() {
        return purpose;
    }

    public String getCiphertext() {
        return ciphertext;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
