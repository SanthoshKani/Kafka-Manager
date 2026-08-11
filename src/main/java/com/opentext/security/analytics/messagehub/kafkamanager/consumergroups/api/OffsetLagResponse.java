package com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api;

public record OffsetLagResponse(
        String topic, int partition, long committedOffset, long endOffset, long lag, boolean missingCommit) {}
