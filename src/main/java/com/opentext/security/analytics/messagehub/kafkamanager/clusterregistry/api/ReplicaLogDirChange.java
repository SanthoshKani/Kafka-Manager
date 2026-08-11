package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import jakarta.validation.constraints.NotBlank;

public record ReplicaLogDirChange(
        @NotBlank String topic,
        int partition,
        int brokerId,
        @NotBlank String logDir) {}
