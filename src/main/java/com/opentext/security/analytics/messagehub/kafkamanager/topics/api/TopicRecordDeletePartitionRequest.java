package com.opentext.security.analytics.messagehub.kafkamanager.topics.api;

import jakarta.validation.constraints.Min;

public record TopicRecordDeletePartitionRequest(
        @Min(0) int partition, @Min(0) long beforeOffset) {}
