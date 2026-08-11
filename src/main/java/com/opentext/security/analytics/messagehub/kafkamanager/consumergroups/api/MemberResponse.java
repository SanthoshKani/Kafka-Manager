package com.opentext.security.analytics.messagehub.kafkamanager.consumergroups.api;

import java.util.List;

public record MemberResponse(String consumerId, String clientId, String host, List<String> assignments) {}
