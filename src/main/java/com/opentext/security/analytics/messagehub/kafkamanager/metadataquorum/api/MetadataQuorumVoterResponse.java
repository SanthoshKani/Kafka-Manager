package com.opentext.security.analytics.messagehub.kafkamanager.metadataquorum.api;

public record MetadataQuorumVoterResponse(int replicaId, long logEndOffset, long lastFetchTimestamp) {}
