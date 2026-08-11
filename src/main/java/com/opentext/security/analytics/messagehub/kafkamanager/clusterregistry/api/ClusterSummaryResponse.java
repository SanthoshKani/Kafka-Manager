package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record ClusterSummaryResponse(
        UUID id,
        String displayName,
        String description,
        String bootstrapServers,
        boolean enabled,
        String environment,
        String ownerTeam,
        List<String> tags,
        Instant lastSuccessfulValidationAt,
        String lastValidationErrorSummary,
        String observedKafkaClusterId,
        Instant createdAt,
        Instant updatedAt,
        long version) {}
