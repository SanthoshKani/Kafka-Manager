package com.opentext.security.analytics.messagehub.kafkamanager.topics.api;

import java.util.List;

public record TopicPartitionResponse(
        int partition,
        Integer leader,
        List<Integer> replicas,
        List<Integer> isr,
        boolean offline,
        boolean underReplicated) {}
