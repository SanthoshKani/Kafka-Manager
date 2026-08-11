package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record PartitionReassignmentRequest(@NotEmpty List<@Valid PartitionReassignmentChange> changes) {}
