package com.opentext.security.analytics.messagehub.kafkamanager.metadataquorum.api;

public record MetadataQuorumObserverResponse(int replicaId, long logEndOffset) {}
