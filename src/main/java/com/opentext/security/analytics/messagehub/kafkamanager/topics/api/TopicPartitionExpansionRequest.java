package com.opentext.security.analytics.messagehub.kafkamanager.topics.api;

import jakarta.validation.constraints.Min;
import java.util.List;

public record TopicPartitionExpansionRequest(@Min(1) int totalPartitions, List<List<Integer>> replicaAssignments) {}
