package com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api;

public record ConsumerGroupSummaryResponse(String groupId, String state, String type, int members, long partitions) {}
