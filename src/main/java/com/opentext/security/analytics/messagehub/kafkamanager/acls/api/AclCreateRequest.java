package com.opentext.security.analytics.messagehub.kafkamanager.acls.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record AclCreateRequest(@NotEmpty List<@Valid AclEntryRequest> bindings) {}
