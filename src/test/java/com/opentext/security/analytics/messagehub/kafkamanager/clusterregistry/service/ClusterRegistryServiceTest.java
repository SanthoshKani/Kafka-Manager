package com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api.ClusterDetailResponse;
import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api.RegisterClusterRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.domain.ClusterEntity;
import com.opentext.security.analytics.messagehub.kafkamanager.common.JsonSupport;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.AdminClientRegistry;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaClientPropertyPolicyService;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@MockitoSettings(strictness = Strictness.LENIENT)
@org.junit.jupiter.api.extension.ExtendWith(MockitoExtension.class)
class ClusterRegistryServiceTest {

    private final KafkaManagerProperties properties = new KafkaManagerProperties(
            "test",
            new KafkaManagerProperties.Security(
                    "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
                    new KafkaManagerProperties.BasicAuth("testuser", "testpass"),
                    new KafkaManagerProperties.OAuth2ResourceServer("", "")),
            new KafkaManagerProperties.Admin(4, Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1)),
            new KafkaManagerProperties.Operations(Duration.ofSeconds(1), Duration.ofSeconds(1)),
            new KafkaManagerProperties.ClusterRegistry(50, 8),
            new KafkaManagerProperties.RateLimit(true, 100, Duration.ofMinutes(1), "X-Client-Id"));
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    ClusterStore clusterRepository;

    @Mock
    SecretStoreService secretStoreService;

    @Mock
    KafkaAdminExecutionService adminExecutionService;

    @Mock
    AdminClientRegistry adminClientRegistry;

    @Mock
    KafkaClientPropertyPolicyService propertyPolicyService;

    @Test
    void listMapsEntitiesToSummaries() {
        ClusterEntity entity = new ClusterEntity();
        entity.setId(UUID.randomUUID());
        entity.setDisplayName("prod");
        entity.setBootstrapServers("localhost:9092");
        entity.setEnabled(true);
        entity.setClientPropertiesAllowlistJson(JsonSupport.toJson(objectMapper, List.of()));
        entity.setTagsJson(JsonSupport.toJson(objectMapper, List.of("core")));
        when(clusterRepository.findAll(PageRequest.of(0, 10))).thenReturn(new PageImpl<>(List.of(entity)));

        ClusterRegistryService service = new ClusterRegistryService(
                clusterRepository,
                secretStoreService,
                adminExecutionService,
                adminClientRegistry,
                propertyPolicyService,
                properties,
                objectMapper);

        assertThat(service.list(0, 10).getContent()).hasSize(1);
    }

    @Test
    void registerNormalizesListenerPrefixedEndpoints() {
        ClusterRegistryService service = new ClusterRegistryService(
                clusterRepository,
                secretStoreService,
                adminExecutionService,
                adminClientRegistry,
                propertyPolicyService,
                properties,
                objectMapper);

        ClusterDetailResponse response = service.register(new RegisterClusterRequest(
                "bootstrap",
                "demo",
                "BROKER://broker-1:9092, BROKER://broker-2:9092",
                "CONTROLLER://controller-1:9093",
                "sasl_ssl",
                "scram-sha-512",
                "admin",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                List.of(),
                "dev",
                "platform",
                List.of("blue"),
                true,
                Duration.ofSeconds(5),
                Duration.ofSeconds(30),
                Duration.ofSeconds(60),
                Map.of(),
                null,
                false));

        assertThat(response.bootstrapServers()).isEqualTo("broker-1:9092,broker-2:9092");
        assertThat(response.controllerBootstrapEndpoints()).isEqualTo("controller-1:9093");
        assertThat(response.securityProtocol()).isEqualTo("SASL_SSL");
    }
}
