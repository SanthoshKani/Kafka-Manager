package com.opentext.security.analytics.messagehub.kafkamanager.config;

import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaEndpointSupport;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@Profile("prod")
public class ProdKafkaAdminClientPropertiesFactory implements KafkaAdminClientPropertiesFactory {

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
        adminClientProperties.put(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                KafkaEndpointSupport.normalizeSecurityProtocol(admin.securityProtocol()));

        KafkaManagerProperties.Ssl ssl = admin.ssl();
        if (ssl != null) {
            putIfNotBlank(adminClientProperties, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ssl.trustStore());
            putIfNotBlank(adminClientProperties, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ssl.trustStorePassword());
            putIfNotBlank(adminClientProperties, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, ssl.trustStoreType());
            putIfNotBlank(adminClientProperties, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ssl.keyStore());
            putIfNotBlank(adminClientProperties, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ssl.keyStorePassword());
            putIfNotBlank(adminClientProperties, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, ssl.keyStoreType());
            putIfNotBlank(
                    adminClientProperties,
                    SslConfigs.SSL_KEY_PASSWORD_CONFIG,
                    ssl.keyPassword() == null || ssl.keyPassword().isBlank() ? ssl.keyStorePassword() : ssl.keyPassword());
            putIfNotBlank(
                    adminClientProperties,
                    SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                    ssl.endpointIdentificationAlgorithm());
            putIfNotBlank(adminClientProperties, SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, ssl.enabledProtocols());
        }

        return adminClientProperties;
    }

    private static void putIfNotBlank(Map<String, Object> target, String key, String value) {
        if (value != null && !value.isBlank()) {
            target.put(key, value);
        }
    }
}
