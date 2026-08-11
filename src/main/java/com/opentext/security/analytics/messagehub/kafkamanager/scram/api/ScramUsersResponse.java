package com.opentext.security.analytics.messagehub.kafkamanager.scram.api;

import java.util.List;

public record ScramUsersResponse(List<ScramCredentialResponse> credentials) {}
