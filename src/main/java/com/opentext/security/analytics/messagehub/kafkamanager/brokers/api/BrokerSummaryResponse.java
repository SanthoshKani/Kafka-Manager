package com.opentext.security.analytics.messagehub.kafkamanager.brokers.api;

import java.util.List;

public record BrokerSummaryResponse(
        int brokerId, String host, int port, boolean controller, boolean fenced, List<String> warnings) {}
