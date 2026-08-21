package com.opentext.security.analytics.messagehub.kafkamanager.config;

import org.apache.kafka.clients.admin.Admin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaAdminClientConfiguration {

    @Bean(destroyMethod = "close")
    Admin kafkaAdminClient(KafkaManagerProperties properties, KafkaAdminClientPropertiesFactory propertiesFactory) {
        return Admin.create(propertiesFactory.create(properties));
    }
}

