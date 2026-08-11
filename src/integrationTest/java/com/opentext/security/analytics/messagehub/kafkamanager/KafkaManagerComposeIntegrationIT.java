package com.opentext.security.analytics.messagehub.kafkamanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.api.*;
import com.opentext.security.analytics.messagehub.kafkamanager.clusterregistry.service.ClusterStore;
import com.opentext.security.analytics.messagehub.kafkamanager.common.TopicPartitionRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.api.SubmitOperationRequest;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.domain.OperationEventStore;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.domain.OperationState;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.domain.OperationStore;
import com.opentext.security.analytics.messagehub.kafkamanager.topics.api.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@SpringBootTest
@ActiveProfiles("it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaManagerComposeIntegrationIT {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    ClusterStore clusterRepository;

    @Autowired
    OperationStore operationRepository;

    @Autowired
    OperationEventStore operationEventRepository;

    @Autowired
    WebApplicationContext context;

    private MockMvc mockMvc;
    private UUID clusterId;
    private String basicAuthHeader;

    @BeforeAll
    void setUp() throws Exception {
        mockMvc = MockMvcBuilders.webAppContextSetup(context)
                .apply(springSecurity())
                .build();
        operationEventRepository.deleteAll();
        operationRepository.deleteAll();
        clusterRepository.deleteAll();
        // Use Basic Auth for integration tests
        basicAuthHeader = "Basic " + Base64.getEncoder().encodeToString("admin:admin".getBytes(StandardCharsets.UTF_8));
        clusterId = registerCluster();
    }

    @AfterAll
    void tearDown() {
        operationEventRepository.deleteAll();
        operationRepository.deleteAll();
        clusterRepository.deleteAll();
    }

    @Test
    void clusterRegistrationValidationAndCapabilitiesWorkAgainstComposeKafka() throws Exception {
        JsonNode validation = performJson(post("/api/v1/clusters/{clusterId}/actions/validate", clusterId), 200);
        assertThat(validation.path("valid").asBoolean()).isTrue();
        assertThat(validation.path("clusterId").asText()).isNotBlank();
        assertThat(validation.path("nodes").isArray()).isTrue();
        assertThat(validation.path("capabilityReport").isArray()).isTrue();

        JsonNode brokers = performJson(get("/api/v1/clusters/{clusterId}/brokers", clusterId), 200);
        assertThat(brokers.isArray()).isTrue();
        assertThat(brokers).hasSizeGreaterThan(0);
        assertThat(brokers.get(0).path("brokerId").asInt()).isEqualTo(1);

        JsonNode capabilities = performJson(get("/api/v1/clusters/{clusterId}/capabilities", clusterId), 200);
        assertThat(capabilities.path("controller").asText()).isNotBlank();
        assertThat(capabilities.path("metadataQuorum").isArray()).isTrue();
    }

    @Test
    void topicLifecycleAndConfigMutationsWorkAgainstComposeKafka() throws Exception {
        String topic = uniqueName("topic");
        createTopic(topic);

        JsonNode description =
                performJson(get("/api/v1/clusters/{clusterId}/topics/{topicName}", clusterId, topic), 200);
        assertThat(description.path("partitions").asInt()).isEqualTo(1);
        assertThat(description.path("replicationFactor").asInt()).isEqualTo(1);

        performNoContent(post("/api/v1/clusters/{clusterId}/topics/{topicName}/partitions", clusterId, topic)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(new TopicPartitionExpansionRequest(2, null))));

        JsonNode afterExpansion =
                performJson(get("/api/v1/clusters/{clusterId}/topics/{topicName}", clusterId, topic), 200);
        assertThat(afterExpansion.path("partitions").asInt()).isEqualTo(2);

        performNoContent(patch("/api/v1/clusters/{clusterId}/topics/{topicName}/configs", clusterId, topic)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(new TopicConfigMutationBatchRequest(List.of(
                        new TopicConfigMutationRequest("retention.ms", "60000", ConfigMutationOperation.SET))))));

        JsonNode configs =
                performJson(get("/api/v1/clusters/{clusterId}/topics/{topicName}/configs", clusterId, topic), 200);
        assertThat(configs.path("retention.ms").asText()).isEqualTo("60000");

        deleteTopic(topic);
    }

    @Test
    void directTopicCrudOffsetsAndTruncationWorkAgainstComposeKafka() throws Exception {
        String topic = uniqueName("direct-topic");
        performNoContent(
                post("/api/v1/clusters/{clusterId}/topics", clusterId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(
                                objectMapper.writeValueAsString(new TopicCreateRequest(topic, 1, (short) 1, Map.of()))),
                204);

        JsonNode offsets = performJson(
                get("/api/v1/clusters/{clusterId}/topics/{topicName}/offsets", clusterId, topic)
                        .param("mode", "LATEST"),
                200);
        assertThat(offsets.isArray()).isTrue();
        assertThat(offsets).hasSize(1);

        produce(topic, "key-1", "value-1");

        performNoContent(
                post("/api/v1/clusters/{clusterId}/topics/{topicName}/records/delete", clusterId, topic)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(
                                new TopicRecordDeleteRequest(List.of(new TopicRecordDeletePartitionRequest(0, 1L))))),
                204);

        performNoContent(delete("/api/v1/clusters/{clusterId}/topics/{topicName}", clusterId, topic), 204);
    }

    @Test
    void readOnlyClusterAdminSurfacesWorkAgainstComposeKafka() throws Exception {
        JsonNode quorum = performJson(get("/api/v1/clusters/{clusterId}/metadata-quorum", clusterId), 200);
        assertThat(quorum.path("highWatermark").asLong()).isGreaterThanOrEqualTo(0L);

        JsonNode clientMetrics = performJson(get("/api/v1/clusters/{clusterId}/client-metrics", clusterId), 200);
        assertThat(clientMetrics.isArray()).isTrue();
    }

    @Test
    void consumerGroupWriteOpsWorkAgainstComposeKafka() throws Exception {
        String topic = uniqueName("group-topic");
        createTopic(topic);
        produce(topic, "key-1", "value-1");

        String groupId = uniqueName("group");
        try (KafkaConsumer<String, String> consumer = consumer(groupId, uniqueName("member"), topic)) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            assertThat(records).isNotEmpty();
            ConsumerRecord<String, String> record = records.iterator().next();
            assertThat(record.value()).isEqualTo("value-1");

            JsonNode detail =
                    performJson(get("/api/v1/clusters/{clusterId}/consumer-groups/{groupId}", clusterId, groupId), 200);
            assertThat(detail.path("members").isArray()).isTrue();
            assertThat(detail.path("members")).hasSizeGreaterThan(0);
        }

        await().atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    JsonNode emptyGroup = performJson(
                            get("/api/v1/clusters/{clusterId}/consumer-groups/{groupId}", clusterId, groupId), 200);
                    assertThat(emptyGroup.path("members").size()).isEqualTo(0);
                });

        performNoContent(delete("/api/v1/clusters/{clusterId}/consumer-groups/{groupId}", clusterId, groupId));

        deleteTopic(topic);
    }

    @Test
    void operationSubmissionCancelAndRetryWorkAgainstComposeKafka() throws Exception {
        String topic = uniqueName("operation-topic");
        JsonNode submitted = performJson(
                post("/api/v1/clusters/{clusterId}/operations", clusterId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new SubmitOperationRequest(
                                "TOPIC_CREATE",
                                true,
                                "integration-suite",
                                "integration-suite",
                                "dry-run-" + topic,
                                topic,
                                Map.of("partitions", 1, "replicationFactor", 1)))),
                200);
        UUID operationId = UUID.fromString(submitted.path("id").asText());
        assertThat(submitted.path("currentState").asText()).isEqualTo("VALIDATING");

        JsonNode events = performJson(
                get("/api/v1/clusters/{clusterId}/operations/{operationId}/events", clusterId, operationId), 200);
        assertThat(events.isArray()).isTrue();
        assertThat(events.size()).isGreaterThan(0);

        performNoContent(
                post("/api/v1/clusters/{clusterId}/operations/{operationId}/cancel", clusterId, operationId), 200);

        operationRepository.findById(operationId).ifPresent(entity -> {
            entity.setCurrentState(OperationState.FAILED);
            operationRepository.saveAndFlush(entity);
        });

        performNoContent(
                post("/api/v1/clusters/{clusterId}/operations/{operationId}/retry", clusterId, operationId), 200);

        JsonNode retried =
                performJson(get("/api/v1/clusters/{clusterId}/operations/{operationId}", clusterId, operationId), 200);
        assertThat(retried.path("currentState").asText()).isEqualTo("PENDING");
        assertThat(retried.path("retryCount").asInt()).isEqualTo(1);
    }

    @Test
    void clusterAdminActionsWorkAgainstComposeKafka() throws Exception {
        String topic = uniqueName("admin-topic");
        createTopic(topic);

        performNoContent(post("/api/v1/clusters/{clusterId}/actions/leader-election", clusterId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(new LeaderElectionRequest(
                        ElectionType.PREFERRED, List.of(new TopicPartitionRequest(topic, 0))))));

        performNoContent(put("/api/v1/clusters/{clusterId}/actions/partition-reassignments", clusterId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(new PartitionReassignmentRequest(
                        List.of(new PartitionReassignmentChange(topic, 0, List.of(1), false))))));

        JsonNode reassignments =
                performJson(get("/api/v1/clusters/{clusterId}/actions/partition-reassignments", clusterId), 200);
        assertThat(reassignments.isArray()).isTrue();

        JsonNode logDirs = performJson(
                get("/api/v1/clusters/{clusterId}/actions/log-dirs", clusterId).param("brokerIds", "1"), 200);
        assertThat(logDirs.isArray()).isTrue();
        assertThat(logDirs).hasSizeGreaterThan(0);
        String logDir = logDirs.get(0).path("logDir").asText();

        performNoContent(put("/api/v1/clusters/{clusterId}/actions/log-dirs", clusterId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(
                        new ReplicaLogDirRequest(List.of(new ReplicaLogDirChange(topic, 0, 1, logDir))))));

        deleteTopic(topic);
    }

    private UUID registerCluster() throws Exception {
        RegisterClusterRequest request = new RegisterClusterRequest(
                "it-cluster",
                "compose-backed integration cluster",
                BOOTSTRAP_SERVERS,
                BOOTSTRAP_SERVERS,
                "PLAINTEXT",
                (String) null,
                (String) null,
                (String) null,
                (String) null,
                (String) null,
                (String) null,
                (String) null,
                (String) null,
                (String) null,
                (String) null,
                (String) null,
                (String) null,
                List.of("request.timeout.ms"),
                "integration",
                "platform",
                List.of("it", "compose"),
                true,
                Duration.ofSeconds(5),
                Duration.ofSeconds(5),
                Duration.ofSeconds(30),
                Map.<String, String>of(),
                "integration-cluster-register",
                false);
        JsonNode response = performJson(
                post("/api/v1/clusters")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)),
                201);
        return UUID.fromString(response.path("id").asText());
    }

    private void createTopic(String topic) throws Exception {
        try (Admin admin = Admin.create(adminProperties())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)), new CreateTopicsOptions())
                    .all()
                    .get(30, TimeUnit.SECONDS);
        }
    }

    private void deleteTopic(String topic) throws Exception {
        try (Admin admin = Admin.create(adminProperties())) {
            admin.deleteTopics(List.of(topic), new DeleteTopicsOptions()).all().get(30, TimeUnit.SECONDS);
        }
    }

    private Properties adminProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("security.protocol", "PLAINTEXT");
        properties.put("request.timeout.ms", 5000);
        properties.put("default.api.timeout.ms", 30000);
        return properties;
    }

    private KafkaConsumer<String, String> consumer(String groupId, String instanceId, String topic) {
        java.util.Properties properties = new java.util.Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private void produce(String topic, String key, String value) throws Exception {
        java.util.Properties properties = new java.util.Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
            producer.flush();
        }
    }

    private JsonNode performJson(MockHttpServletRequestBuilder request, int expectedStatus) throws Exception {
        MvcResult result = mockMvc.perform(request.header(HttpHeaders.AUTHORIZATION, basicAuthHeader))
                .andReturn();
        int actualStatus = result.getResponse().getStatus();
        if (actualStatus != expectedStatus) {
            throw new AssertionError("Unexpected status for "
                    + result.getRequest().getMethod()
                    + " "
                    + result.getRequest().getRequestURI()
                    + ": expected "
                    + expectedStatus
                    + " but was "
                    + actualStatus
                    + " body="
                    + result.getResponse().getContentAsString());
        }
        String content = result.getResponse().getContentAsString();
        return content == null || content.isBlank() ? objectMapper.createObjectNode() : objectMapper.readTree(content);
    }

    private void performNoContent(MockHttpServletRequestBuilder request) throws Exception {
        performStatus(request, 204);
    }

    private void performNoContent(MockHttpServletRequestBuilder request, int expectedStatus) throws Exception {
        performStatus(request, expectedStatus);
    }

    private void performStatus(MockHttpServletRequestBuilder request, int expectedStatus) throws Exception {
        MvcResult result = mockMvc.perform(request.header(HttpHeaders.AUTHORIZATION, basicAuthHeader))
                .andReturn();
        int actualStatus = result.getResponse().getStatus();
        if (actualStatus != expectedStatus) {
            throw new AssertionError("Unexpected status for "
                    + result.getRequest().getMethod()
                    + " "
                    + result.getRequest().getRequestURI()
                    + ": expected "
                    + expectedStatus
                    + " but was "
                    + actualStatus
                    + " body="
                    + result.getResponse().getContentAsString());
        }
    }

    private String uniqueName(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
