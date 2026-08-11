package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import java.util.List;

public record PartitionReassignmentResponse(
        String topic,
        int partition,
        List<Integer> replicas,
        List<Integer> addingReplicas,
        List<Integer> removingReplicas) {}
