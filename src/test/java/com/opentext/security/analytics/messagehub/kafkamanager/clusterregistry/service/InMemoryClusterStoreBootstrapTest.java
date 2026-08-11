package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.domain.ClusterEntity;
import com.opentext.security.analytics.messagehub.kafkamanager.common.JsonSupport;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryClusterStoreBootstrapTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void initFromEnvLoadsFullClusterConfiguration() throws Exception {
        UUID id = UUID.randomUUID();
        UUID credentialSecretId = UUID.randomUUID();
        UUID truststoreSecretId = UUID.randomUUID();
        UUID keystoreSecretId = UUID.randomUUID();
        UUID truststorePasswordSecretId = UUID.randomUUID();
        UUID keystorePasswordSecretId = UUID.randomUUID();
        UUID keyPasswordSecretId = UUID.randomUUID();

        Map<String, Object> cluster = new LinkedHashMap<>();
        cluster.put("id", id.toString());
        cluster.put("displayName", "bootstrap-cluster");
        cluster.put("description", "loaded from startup JSON");
        cluster.put("bootstrapServers", "BROKER://broker-1:9092, BROKER://broker-2:9092");
        cluster.put("controllerBootstrapEndpoints", "CONTROLLER://controller-1:9093");
        cluster.put("securityProtocol", "sasl_ssl");
        cluster.put("saslMechanism", "scram-sha-512");
        cluster.put("username", "cluster-admin");
        cluster.put("credentialSecretId", credentialSecretId.toString());
        cluster.put("truststoreSecretId", truststoreSecretId.toString());
        cluster.put("keystoreSecretId", keystoreSecretId.toString());
        cluster.put("truststorePasswordSecretId", truststorePasswordSecretId.toString());
        cluster.put("keystorePasswordSecretId", keystorePasswordSecretId.toString());
        cluster.put("keyPasswordSecretId", keyPasswordSecretId.toString());
        cluster.put("sslEndpointIdentificationAlgorithm", "https");
        cluster.put("sslEnabledProtocols", "TLSv1.2,TLSv1.3");
        cluster.put("sslTruststoreType", "PKCS12");
        cluster.put("sslKeystoreType", "PKCS12");
        cluster.put("clientPropertiesAllowlist", List.of("client.id", "request.timeout.ms"));
        cluster.put("environment", "prod");
        cluster.put("ownerTeam", "platform");
        cluster.put("tags", List.of("bootstrap", "kafka"));
        cluster.put("enabled", true);
        cluster.put("connectionTimeoutMs", 5000L);
        cluster.put("requestTimeoutMs", 10000L);
        cluster.put("operationTimeoutMs", 15000L);

        String clustersJson = objectMapper.writeValueAsString(List.of(cluster));

        InMemoryClusterStore store = new InMemoryClusterStore(objectMapper);
        ReflectionTestUtils.setField(store, "clustersJson", clustersJson);

        store.initFromEnv();

        assertThat(store.findAll(PageRequest.of(0, 10)).getContent()).hasSize(1);
        ClusterEntity entity = store.findById(id).orElseThrow();
        assertThat(entity.getDisplayName()).isEqualTo("bootstrap-cluster");
        assertThat(entity.getBootstrapServers()).isEqualTo("broker-1:9092,broker-2:9092");
        assertThat(entity.getControllerBootstrapEndpoints()).isEqualTo("controller-1:9093");
        assertThat(entity.getSecurityProtocol()).isEqualTo("SASL_SSL");
        assertThat(entity.getSaslMechanism()).isEqualTo("scram-sha-512");
        assertThat(entity.getCredentialSecretId()).isEqualTo(credentialSecretId);
        assertThat(entity.getTruststoreSecretId()).isEqualTo(truststoreSecretId);
        assertThat(entity.getKeystoreSecretId()).isEqualTo(keystoreSecretId);
        assertThat(entity.getTruststorePasswordSecretId()).isEqualTo(truststorePasswordSecretId);
        assertThat(entity.getKeystorePasswordSecretId()).isEqualTo(keystorePasswordSecretId);
        assertThat(entity.getKeyPasswordSecretId()).isEqualTo(keyPasswordSecretId);
        assertThat(entity.getSslEndpointIdentificationAlgorithm()).isEqualTo("https");
        assertThat(entity.getSslEnabledProtocols()).isEqualTo("TLSv1.2,TLSv1.3");
        assertThat(entity.getSslTruststoreType()).isEqualTo("PKCS12");
        assertThat(entity.getSslKeystoreType()).isEqualTo("PKCS12");
        assertThat(JsonSupport.toStringList(objectMapper, entity.getClientPropertiesAllowlistJson()))
                .containsExactly("client.id", "request.timeout.ms");
        assertThat(JsonSupport.toStringList(objectMapper, entity.getTagsJson()))
                .containsExactly("bootstrap", "kafka");
        assertThat(entity.isEnabled()).isTrue();
        assertThat(entity.getConnectionTimeoutMs()).isEqualTo(5000L);
        assertThat(entity.getRequestTimeoutMs()).isEqualTo(10000L);
        assertThat(entity.getOperationTimeoutMs()).isEqualTo(15000L);
    }
}


