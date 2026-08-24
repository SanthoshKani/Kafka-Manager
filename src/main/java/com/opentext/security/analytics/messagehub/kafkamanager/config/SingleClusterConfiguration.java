package com.opentext.security.analytics.messagehub.kafkamanager.config;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides a deterministic default cluster id for single-cluster deployments.
 */
@Configuration
public class SingleClusterConfiguration {

    @Bean
    public UUID defaultClusterId(KafkaManagerProperties properties) {
        // Derive a stable UUID from the bootstrap servers string so the id is deterministic
        String bootstrap = properties.admin().bootstrapServers();
        if (bootstrap == null) {
            bootstrap = "localhost:9092";
        }
        return UUID.nameUUIDFromBytes(bootstrap.getBytes(StandardCharsets.UTF_8));
    }
}
