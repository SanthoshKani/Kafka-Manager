package com.opentext.security.analytics.messagehub.kafkamanager.config;

import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaEndpointSupport;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@Profile("local")
public class LocalKafkaAdminClientPropertiesFactory implements KafkaAdminClientPropertiesFactory {

    @Override
    public Map<String, Object> create(KafkaManagerProperties properties) {
        KafkaManagerProperties.Admin admin = properties.admin();
        Map<String, Object> adminClientProperties = new HashMap<>();
        adminClientProperties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaEndpointSupport.normalizeEndpointList(admin.bootstrapServers()));
        adminClientProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, properties.serviceName() + "-admin");
        adminClientProperties.put(
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.toIntExact(admin.defaultRequestTimeout().toMillis()));
        adminClientProperties.put(
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                Math.toIntExact(admin.defaultOperationTimeout().toMillis()));
        adminClientProperties.put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, 300_000L);
        adminClientProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        return adminClientProperties;
    }
}
