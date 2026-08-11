package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import java.util.List;

public record CapabilityReportResponse(
        String clusterId,
        String controller,
        List<String> brokerNodes,
        List<String> featureLevels,
        List<String> finalizedFeatures,
        List<String> metadataQuorum,
        List<String> limitations) {}
