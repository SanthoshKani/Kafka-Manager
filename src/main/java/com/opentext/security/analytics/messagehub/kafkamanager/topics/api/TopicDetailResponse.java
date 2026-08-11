package com.opentext.security.analytics.messagehub.kafkamanager.topics.api;

import java.util.List;
import java.util.Map;

public record TopicDetailResponse(
        String name,
        boolean internal,
        int partitions,
        short replicationFactor,
        List<TopicPartitionResponse> partitionMetadata,
        Map<String, String> configs,
        List<String> warnings) {}
