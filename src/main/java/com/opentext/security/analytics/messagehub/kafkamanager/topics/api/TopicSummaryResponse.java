package com.opentext.security.analytics.messagehub.kafkamanager.topics.api;

import java.util.List;

public record TopicSummaryResponse(
        String name,
        boolean internal,
        int partitions,
        short replicationFactor,
        List<Integer> partitionIds,
        List<String> warnings) {}
