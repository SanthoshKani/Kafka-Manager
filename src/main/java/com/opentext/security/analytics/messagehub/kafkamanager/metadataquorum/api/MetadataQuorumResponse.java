package com.opentext.security.analytics.messagehub.kafkamanager.metadataquorum.api;

import java.util.List;

public record MetadataQuorumResponse(
        int leaderId,
        long leaderEpoch,
        long highWatermark,
        List<MetadataQuorumVoterResponse> voters,
        List<MetadataQuorumObserverResponse> observers) {}
