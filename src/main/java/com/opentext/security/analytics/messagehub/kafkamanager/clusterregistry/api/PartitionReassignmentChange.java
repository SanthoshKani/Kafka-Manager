package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record PartitionReassignmentChange(
        @NotBlank String topic, int partition, @NotEmpty List<Integer> replicas, boolean cancel) {}
