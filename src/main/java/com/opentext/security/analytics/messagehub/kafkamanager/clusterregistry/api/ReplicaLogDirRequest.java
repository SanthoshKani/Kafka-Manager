package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record ReplicaLogDirRequest(@NotEmpty List<@Valid ReplicaLogDirChange> changes) {}
