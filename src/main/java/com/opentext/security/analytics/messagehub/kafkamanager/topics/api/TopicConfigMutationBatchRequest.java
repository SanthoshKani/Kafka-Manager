package com.opentext.security.analytics.messagehub.kafkamanager.topics.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record TopicConfigMutationBatchRequest(@NotEmpty List<@Valid TopicConfigMutationRequest> changes) {}
