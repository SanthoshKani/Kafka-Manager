package com.opentext.security.analytics.messagehub.kafkamanager.topics.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record TopicRecordDeleteRequest(@NotEmpty List<@Valid TopicRecordDeletePartitionRequest> partitions) {}
