package com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api;

import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record ConsumerGroupMemberRemovalRequest(@NotEmpty List<String> memberIds) {}
