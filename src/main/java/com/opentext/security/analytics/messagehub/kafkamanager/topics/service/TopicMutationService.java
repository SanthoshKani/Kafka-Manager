package com.opentext.security.analytics.messagehub.kafkamanager.topics.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opentext.security.analytics.messagehub.kafkamanager.config.KafkaManagerProperties;
import com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin.KafkaAdminExecutionService;
import com.opentext.security.analytics.messagehub.kafkamanager.operations.service.AdminMutationRecorder;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

/**
 * Service providing JSON-driven topic mutation operations such as create and delete.
 *
 * <p>This service accepts compact JSON payloads for topic create operations and supports
 * dry-run validation. Mutating operations are recorded via {@code AdminMutationRecorder} and
 * executed through {@code KafkaAdminExecutionService} to enforce timeouts and error translation.
 */
@Service
public class TopicMutationService {

    private final KafkaAdminExecutionService adminExecutionService;
    private final AdminMutationRecorder mutationRecorder;
    private final KafkaManagerProperties properties;
    private final ObjectMapper objectMapper;

    public TopicMutationService(
            KafkaAdminExecutionService adminExecutionService,
            AdminMutationRecorder mutationRecorder,
            KafkaManagerProperties properties,
            ObjectMapper objectMapper) {
        this.adminExecutionService = adminExecutionService;
        this.mutationRecorder = mutationRecorder;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    /**
     * Create a topic using a JSON payload describing partitions and replication factor.
     *
     * @param clusterId the target Kafka cluster id
     * @param topicName the name of the topic to create
     * @param payloadJson optional JSON string containing fields such as "partitions" and "replicationFactor"
     * @param dryRun when true, only validate the request without performing the creation
     * @throws IllegalArgumentException when the provided payload JSON is invalid
     */
    public void create(UUID clusterId, String topicName, String payloadJson, boolean dryRun) {
        mutationRecorder.record(
                clusterId,
                dryRun ? "dry-run-create-topic" : "create-topic",
                topicName,
                dryRun,
                payloadJson,
                () -> adminExecutionService.execute(
                        clusterId,
                        dryRun ? "dry-run-create-topic" : "create-topic",
                        properties.admin().defaultOperationTimeout(),
                        handle -> {
                            Admin admin = handle.admin();
                            JsonNode payload = parse(payloadJson);
                            int partitions = payload.path("partitions").asInt(1);
                            short replicationFactor =
                                    (short) payload.path("replicationFactor").asInt(1);
                            CreateTopicsOptions options = new CreateTopicsOptions().validateOnly(dryRun);
                            admin.createTopics(List.of(new NewTopic(topicName, partitions, replicationFactor)), options)
                                    .all()
                                    .toCompletionStage()
                                    .toCompletableFuture()
                                    .join();
                            return null;
                        }));
    }

    /**
     * Delete a topic with optional dry-run validation.
     *
     * @param clusterId the target Kafka cluster id
     * @param topicName the name of the topic to delete
     * @param dryRun when true, validate deletion without performing it
     */
    public void delete(UUID clusterId, String topicName, boolean dryRun) {
        mutationRecorder.record(
                clusterId,
                dryRun ? "dry-run-delete-topic" : "delete-topic",
                topicName,
                dryRun,
                topicName,
                () -> adminExecutionService.execute(
                        clusterId,
                        dryRun ? "dry-run-delete-topic" : "delete-topic",
                        properties.admin().defaultOperationTimeout(),
                        handle -> {
                            Admin admin = handle.admin();
                            if (!dryRun) {
                                DeleteTopicsOptions options = new DeleteTopicsOptions().timeoutMs((int) properties
                                        .admin()
                                        .defaultOperationTimeout()
                                        .toMillis());
                                admin.deleteTopics(List.of(topicName), options)
                                        .all()
                                        .toCompletionStage()
                                        .toCompletableFuture()
                                        .join();
                            }
                            return null;
                        }));
    }

    private JsonNode parse(String payloadJson) {
        try {
            return payloadJson == null || payloadJson.isBlank()
                    ? objectMapper.createObjectNode()
                    : objectMapper.readTree(payloadJson);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid operation payload", exception);
        }
    }
}
