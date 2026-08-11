package com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin;

import com.opentext.security.analytics.messagehub.kafkamanager.common.InvalidOperationException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class KafkaClientPropertyPolicyService {

    private static final Set<String> ALLOWED_KEYS = Set.of(
            "client.id",
            "request.timeout.ms",
            "default.api.timeout.ms",
            "connections.max.idle.ms",
            "metadata.max.age.ms",
            "reconnect.backoff.ms",
            "reconnect.backoff.max.ms",
            "retry.backoff.ms",
            "security.protocol",
            "sasl.mechanism",
            "sasl.jaas.config",
            "ssl.protocol",
            "ssl.enabled.protocols",
            "ssl.endpoint.identification.algorithm",
            "ssl.truststore.location",
            "ssl.truststore.password",
            "ssl.key.password",
            "ssl.keystore.location",
            "ssl.keystore.password",
            "ssl.truststore.type",
            "ssl.keystore.type");

    public void validate(Map<String, String> clientProperties, List<String> allowlist) {
        if (clientProperties == null || clientProperties.isEmpty()) {
            return;
        }
        for (String key : clientProperties.keySet()) {
            if (!allowlist.contains(key) && !ALLOWED_KEYS.contains(key)) {
                throw new InvalidOperationException("Client property '" + key + "' is not allowed");
            }
        }
    }
}
