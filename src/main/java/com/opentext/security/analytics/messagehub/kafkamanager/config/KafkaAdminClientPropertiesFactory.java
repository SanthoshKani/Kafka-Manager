package com.opentext.security.analytics.messagehub.kafkamanager.config;

import java.util.Map;

public interface KafkaAdminClientPropertiesFactory {
    Map<String, Object> create(KafkaManagerProperties properties);
}
