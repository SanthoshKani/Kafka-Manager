package com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record ConsumerGroupOffsetUpdateRequest(@NotEmpty List<@Valid ConsumerGroupOffsetUpdate> offsets) {}
