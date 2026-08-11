package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import java.util.List;

public record BrokerLogDirResponse(
        int brokerId,
        String logDir,
        long totalBytes,
        long usableBytes,
        String error,
        List<BrokerLogDirReplicaResponse> replicas) {}
