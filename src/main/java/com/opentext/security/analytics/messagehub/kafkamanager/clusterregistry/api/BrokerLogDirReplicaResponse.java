package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

public record BrokerLogDirReplicaResponse(String topic, int partition, long size, long offsetLag, boolean future) {}
