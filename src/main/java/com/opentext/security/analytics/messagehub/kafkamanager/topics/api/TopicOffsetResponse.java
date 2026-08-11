package com.opentext.security.analytics.messagehub.kafkamanager.topics.api;

public record TopicOffsetResponse(int partition, long offset, long timestamp) {}
