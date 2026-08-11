package com.opentext.security.analytics.messagehub.kafkamanager.acls.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record AclDeleteRequest(@NotEmpty List<@Valid AclFilterRequest> filters) {}
