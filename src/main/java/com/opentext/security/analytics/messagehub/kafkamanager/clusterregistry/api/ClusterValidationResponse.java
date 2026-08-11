package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import java.util.List;

public record ClusterValidationResponse(
        boolean valid,
        String clusterId,
        String controller,
        List<String> nodes,
        List<String> warnings,
        List<String> capabilityReport,
        String failureCategory,
        String failureSummary) {}
